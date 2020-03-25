package kafka.producer;

import kafka.api.MultiProducerRequest;
import kafka.api.ProducerRequest;
import kafka.api.RequestKeys;
import kafka.message.ByteBufferMessageSet;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
import kafka.network.BoundedByteBufferSend;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Random;

import static com.sun.corba.se.impl.naming.cosnaming.TransientNameServer.trace;

public class SyncProducer {

    private static Logger logger = Logger.getLogger(SyncProducer.class);

    public static short RequestKey = 0;
    public static Random randomGenerator = new Random();

    SyncProducerConfig config;

    public SyncProducer(SyncProducerConfig config){
        this.config = config;
        lastConnectionTime = System.currentTimeMillis() - (long)randomGenerator.nextDouble() * config.reconnectInterval;
    }

    private int MaxConnectBackoffMs = 60000;
    private SocketChannel channel  = null;
    private int sentOnConnection = 0;
    /** make time-based reconnect starting at a random time **/
    private long lastConnectionTime ;
    private Object lock = new Object();
    private volatile boolean shutdown = false;

    public SyncProducerConfig config() {
        return config;
    }

    private void verifySendBuffer(ByteBuffer buffer) {
        /**
         * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
         * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
         * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
         */
        if (logger.isDebugEnabled()) {
            logger.trace("verifying sendbuffer of size " + buffer.limit());
            short requestTypeId = buffer.getShort();
            if (requestTypeId == RequestKeys.MultiProduce) {
                try {
                    MultiProducerRequest request = MultiProducerRequest.readFrom(buffer);
                    for (ProducerRequest produce : request.produces()) {
                        try {
                            Iterator<MessageAndOffset> iterator = produce.messages().iterator();
                            while (iterator.hasNext()){
                                MessageAndOffset messageAndOffset = iterator.next();
                                if (!messageAndOffset.message().isValid())
                                    throw new InvalidMessageException("Message for topic " + produce.topic() + " is invalid");
                            }
                        } catch (Throwable e){
                            logger.error("error iterating messages ", e);
                        }
                    }
                }catch (Throwable e){
                    logger.error("error verifying sendbuffer ", e);
                }
            }
        }
    }

    /**
     * Common functionality for the public send methods
     */
    private void send(BoundedByteBufferSend send) throws Exception{
        synchronized (lock){
            verifySendBuffer(send.buffer().slice());
            getOrMakeConnection();
            try {
                send.writeCompletely(channel);
            } catch (IOException e){
                disconnect();
                throw  e;
            }
            // TODO: do we still need this?
            sentOnConnection += 1;

            if(sentOnConnection >= config.reconnectInterval || (config.reconnectTimeInterval >= 0 && System.currentTimeMillis() - lastConnectionTime >= config.reconnectTimeInterval)) {
                disconnect();
                channel = connect();
                sentOnConnection = 0;
                lastConnectionTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Send a message
     */
    public void send(String topic, int partition,ByteBufferMessageSet messages) throws Exception{
        messages.verifyMessageSize(config.maxMessageSize);
        int setSize = (int)messages.sizeInBytes();
        logger.trace("Got message set with " + setSize + " bytes to send");
        send(new BoundedByteBufferSend(new ProducerRequest(topic, partition, messages)));
    }

    public void send(String topic, ByteBufferMessageSet messages) throws Exception{
        send(topic, -1, messages);
    }

    public void multiSend(ProducerRequest[] produces) throws Exception{
        //todo
        int setSize = 0;
        for (ProducerRequest request : produces) {
            request.messages().verifyMessageSize(config.maxMessageSize);
            setSize += request.messages().sizeInBytes();
        }
        logger.trace("Got multi message sets with " + setSize + " bytes to send");
        send(new BoundedByteBufferSend(new MultiProducerRequest(produces)));
    }

    public void close()  {
        synchronized(lock) {
            disconnect();
            shutdown = true;
        }
    }


    /**
     * Disconnect from current channel, closing connection.
     * Side effect: channel field is set to null on successful disconnect
     */
    private void disconnect() {
        try {
            if(channel != null) {
                logger.info("Disconnecting from " + config.host + ":" + config.port);
                channel.close();
                channel.socket().close();
                channel = null;
            }
        } catch(Exception e) {
            logger.error("Error on disconnect: ", e);
        }
    }

    private SocketChannel connect() throws Exception{
        int connectBackoffMs = 1;
        long beginTimeMs = System.currentTimeMillis();
        while(channel == null && !shutdown) {
            try {
                channel = SocketChannel.open();
                channel.socket().setSendBufferSize(config.bufferSize);
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(config.socketTimeoutMs);
                channel.socket().setKeepAlive(true);
                channel.connect(new InetSocketAddress(config.host, config.port));
                logger.info("Connected to " + config.host + ":" + config.port + " for producing");
            }
            catch (Exception e){
                disconnect();
                long endTimeMs = System.currentTimeMillis();
                if ( (endTimeMs - beginTimeMs + connectBackoffMs) > config.connectTimeoutMs)
                {
                    logger.error("Producer connection to " +  config.host + ":" + config.port + " timing out after " + config.connectTimeoutMs + " ms", e);
                    throw e;
                }
                logger.error("Connection attempt to " +  config.host + ":" + config.port + " failed, next attempt in " + connectBackoffMs + " ms", e);
                Thread.sleep(connectBackoffMs);
                connectBackoffMs = Math.min(10 * connectBackoffMs, MaxConnectBackoffMs);
            }
        }
        return channel;
    }

    private void getOrMakeConnection() throws Exception{
        if(channel == null) {
            channel = connect();
        }
    }
}

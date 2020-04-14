package kafka.producer;

import kafka.api.*;
import kafka.network.BlockingChannel;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Receive;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;


public class SyncProducer {

    private static Logger logger = Logger.getLogger(SyncProducer.class);


    public SyncProducerConfig config;

    public SyncProducer(SyncProducerConfig config){
        this.config = config;
        blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
                config.sendBufferBytes, config.requestTimeoutMs);
        brokerInfo = "host_%s-port_%s".format(config.host, config.port);
    }

    private BlockingChannel blockingChannel;
    public String brokerInfo ;
    private Object lock = new Object();
    private volatile boolean shutdown = false;

    public SyncProducerConfig config() {
        return config;
    }


    private void verifyRequest(RequestOrResponse request) throws IOException {
        /**
         * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
         * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
         * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
         */
        if (logger.isDebugEnabled()) {
            ByteBuffer buffer = new BoundedByteBufferSend(request).buffer();
            logger.info("verifying sendbuffer of size " + buffer.limit());
            short requestTypeId = buffer.getShort();
            if(requestTypeId == RequestKeys.ProduceKey) {
                ProducerRequest request1 = ProducerRequest.readFrom(buffer);
                logger.info(request1.toString());
            }
        }
    }

    /**
     * Common functionality for the public send methods
     */
    private Receive doSend(RequestOrResponse request, boolean readResponse) throws IOException {
        synchronized(lock) {
            verifyRequest(request);
            getOrMakeConnection();

            Receive response  = null;
            try {
                blockingChannel.send(request);
                if(readResponse)
                    response = blockingChannel.receive();
                else
                    logger.trace("Skipping reading response");
            } catch (IOException e){
                // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
                disconnect();
                throw e;
            }catch (Throwable e1){
                throw e1;
            }
            return response;
        }
    }

    /**
     * Send a message. If the producerRequest had required.request.acks=0, then the
     * returned response object is null
     */
    public ProducerResponse send(ProducerRequest producerRequest) throws IOException {
        Receive response = doSend(producerRequest, producerRequest.requiredAcks == 0? false : true);
        if(producerRequest.requiredAcks != 0)
            return ProducerResponse.readFrom(response.buffer());
        else
            return null;
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) throws IOException {
        Receive response = doSend(request,true);
        return TopicMetadataResponse.readFrom(response.buffer());
    }

    public void close()  {
        synchronized(lock) {
            disconnect();
            shutdown = true;
        }
    }

    private void reconnect() throws IOException {
        disconnect();
        connect();
    }

    /**
     * Disconnect from current channel, closing connection.
     * Side effect: channel field is set to null on successful disconnect
     */
    private void disconnect() {
        try {
            if(blockingChannel.isConnected()) {
                logger.info("Disconnecting from " + config.host + ":" + config.port);
                blockingChannel.disconnect();
            }
        } catch (Exception e){
            logger.error("Error on disconnect: ", e);
        }
    }

    private BlockingChannel connect() throws IOException {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
                logger.info("Connected to " + config.host + ":" + config.port + " for producing");
            } catch (Exception e){
                disconnect();
                logger.error("Producer connection to " +  config.host + ":" + config.port + " unsuccessful", e);
                throw e;
            }
        }
        return blockingChannel;
    }

    private void getOrMakeConnection() throws IOException {
        if(!blockingChannel.isConnected()) {
            connect();
        }
    }


}

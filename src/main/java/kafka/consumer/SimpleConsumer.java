package kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.api.OffsetRequest;
import kafka.message.ByteBufferMessageSet;
import kafka.network.BoundedByteBufferReceive;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Receive;
import kafka.network.Request;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * A consumer of kafka messages
 */
public class SimpleConsumer {

    private static Logger logger = Logger.getLogger(SimpleConsumer.class);

    String host;
    int port;
    int soTimeout;
    int bufferSize;

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
    }

    private SocketChannel channel  = null;
    private Object lock = new Object();

    private SocketChannel connect() throws IOException {
        close();
        InetSocketAddress address = new InetSocketAddress(host, port);

        SocketChannel channel = SocketChannel.open();
        logger.debug("Connected to " + address + " for fetching.");
        channel.configureBlocking(true);
        channel.socket().setReceiveBufferSize(bufferSize);
        channel.socket().setSoTimeout(soTimeout);
        channel.socket().setKeepAlive(true);
        channel.socket().setTcpNoDelay(true);
        channel.connect(address);
        logger.trace("requested receive buffer size=" + bufferSize + " actual receive buffer size= " + channel.socket().getReceiveBufferSize());
        logger.trace("soTimeout=" + soTimeout + " actual soTimeout= " + channel.socket().getSoTimeout());

        return channel;
    }

    private void close(SocketChannel channel) throws IOException {
        logger.debug("Disconnecting from " + channel.socket().getRemoteSocketAddress());
        channel.close();
        channel.socket().close();
    }

    public void close() throws IOException{
        synchronized(lock) {
            if (channel != null)
                close(channel);
            channel = null;
        }
    }

    /**
     *  Fetch a set of messages from a topic.
     *
     *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     *  @return a set of fetched messages
     */
    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException{
        synchronized(lock) {
            getOrMakeConnection();
            Pair<Receive,Integer> response = null;
            try {
                sendRequest(request);
                response = getResponse();
            } catch(IOException e) {
                logger.info("Reconnect in fetch request due to socket error: ", e);
                // retry once
                try {
                    channel = connect();
                    sendRequest(request);
                    response = getResponse();
                }catch (IOException ioe){
                    channel = null;
                    throw ioe;
                }
            }
            return new ByteBufferMessageSet(response.getKey().buffer(), request.offset(),response.getValue());
        }
    }

    /**
     *  Combine multiple fetch requests in one call.
     *
     *  @param fetches  a sequence of fetch requests.
     *  @return a sequence of fetch responses
     */
    public MultiFetchResponse multifetch(FetchRequest... fetches) throws IOException{
        synchronized(lock) {
            getOrMakeConnection();
            Pair<Receive,Integer> response = null;
            try {
                sendRequest(new MultiFetchRequest(fetches));
                response = getResponse();
            } catch (IOException e){
                logger.info("Reconnect in multifetch due to socket error: ", e);
                // retry once
                try {
                    channel = connect();
                    sendRequest(new MultiFetchRequest(fetches));
                    response = getResponse();
                }catch (IOException ioe){
                    channel = null;
                    throw ioe;
                }
            }
            // error code will be set on individual messageset inside MultiFetchResponse
            long[] offsets = new long[fetches.length];
            for(int i = 0;i < fetches.length;i++){
                offsets[i] = fetches[i].offset();
            }
            return  new MultiFetchResponse(response.getKey().buffer(), fetches.length, offsets);
        }
    }

    /**
     *  Get a list of valid offsets (up to maxSize) before the given time.
     *  The result is a list of offsets, in descending order.
     *
     *  @param time: time in millisecs (-1, from the latest offset available, -2 from the smallest offset available)
     *  @return an array of offsets
     */
    public long[]  getOffsetsBefore(String topic,int  partition,long time, int maxNumOffsets) throws IOException{
        synchronized(lock) {
            getOrMakeConnection();
            Pair<Receive,Integer> response = null;
            try {
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                response = getResponse();
            } catch (IOException e){
                    logger.info("Reconnect in get offetset request due to socket error: ", e);
                    // retry once
                    try {
                        channel = connect();
                        sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                        response = getResponse();
                    }catch (IOException ioe){
                        channel = null;
                        throw ioe;
                }
            }
           return OffsetRequest.deserializeOffsetArray(response.getKey().buffer());
        }
    }

    private void sendRequest(Request request) throws IOException {
        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
        send.writeCompletely(channel);
    }

    private Pair<Receive,Integer> getResponse() throws IOException{
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(channel);

        // this has the side effect of setting the initial position of buffer correctly
        int errorCode = response.buffer().getShort();
        return new Pair<>(response,errorCode);
    }

    private void getOrMakeConnection() throws IOException{
        if (channel == null) {
            channel = connect();
        }
    }
}

package kafka.consumer;

import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.network.BlockingChannel;
import kafka.network.BoundedByteBufferReceive;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Receive;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * A consumer of kafka messages
 */
public class SimpleConsumer {

    private static Logger logger = Logger.getLogger(SimpleConsumer.class);

    String host;
    int port;
    int soTimeout;
    int bufferSize;
    String clientId;

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize,String clientId) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.clientId = clientId;

        blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
        brokerInfo = String.format("host_%s-port_%s",host, port);
    }

    private Object lock = new Object();

    private BlockingChannel blockingChannel ;
    String brokerInfo ;
    private volatile boolean isClosed = false;

    private BlockingChannel connect() throws IOException {
        close();
        blockingChannel.connect();
        return blockingChannel;
    }

    private void disconnect() throws IOException {
        if(blockingChannel.isConnected()) {
            logger.debug("Disconnecting from " + host + ":" + port);
            blockingChannel.disconnect();
        }
    }

    private void reconnect() throws IOException {
        disconnect();
        connect();
    }

    public void close() throws IOException {
         synchronized(lock) {
            disconnect();
            isClosed = true;
        }
    }

    public Receive sendRequest(RequestOrResponse request) throws IOException {
        synchronized(lock) {
            getOrMakeConnection();
            Receive response = null;
            try {
                blockingChannel.send(request);
                response = blockingChannel.receive();
            } catch(IOException e) {
                    logger.info(String.format("Reconnect due to socket error: %s",e.getMessage()));
                    // retry once
                    try {
                        reconnect();
                        blockingChannel.send(request);
                        response = blockingChannel.receive();
                    } catch (IOException ioe){
                        disconnect();
                        throw ioe;
                }
            }
           return response;
        }
    }
    public TopicMetadataResponse send(TopicMetadataRequest request) throws IOException {
        Receive response = sendRequest(request);
        return TopicMetadataResponse.readFrom(response.buffer());
    }
    public OffsetResponse getOffsetsBefore(OffsetRequest request) throws IOException{
        return OffsetResponse.readFrom(sendRequest(request).buffer());
    }

    private void getOrMakeConnection() throws IOException{
        if(!isClosed && !blockingChannel.isConnected()) {
            connect();
        }
    }

    /**
     *  Fetch a set of messages from a topic.
     *
     *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     *  @return a set of fetched messages
     */
    public FetchResponse fetch(FetchRequest request) throws IOException {
        Receive response = null;
        FetchResponse fetchResponse = FetchResponse.readFrom(response.buffer());
        return fetchResponse;
    }
    /**
     * Get the earliest or latest offset of a given topic, partition.
     * @param topicAndPartition Topic and partition of which the offset is needed.
     * @param earliestOrLatest A value to indicate earliest or latest offset.
     * @param consumerId Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
     * @return Requested offset.
     */
    public long earliestOrLatestOffset(TopicAndPartition topicAndPartition, long earliestOrLatest, int consumerId) throws Throwable {
        Map<TopicAndPartition, OffsetRequest.PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new OffsetRequest.PartitionOffsetRequestInfo(earliestOrLatest, 1));
        OffsetRequest request = new OffsetRequest(0,requestInfo,
                OffsetRequest.CurrentVersion,
                clientId,
                consumerId);
        OffsetResponse.PartitionOffsetsResponse partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets.get(topicAndPartition);
        long offset = 0;
        if(partitionErrorAndOffset.error == ErrorMapping.NoError){
            offset = partitionErrorAndOffset.offsets.get(0);
        }else{
            throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error);
        }
        return offset;
    }
}

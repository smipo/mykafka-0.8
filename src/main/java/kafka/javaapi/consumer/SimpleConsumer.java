package kafka.javaapi.consumer;

import kafka.api.FetchRequest;
import kafka.javaapi.MultiFetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;

import java.io.IOException;

public class SimpleConsumer {

    String host;
    int port;
    int soTimeout;
    int bufferSize;

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) throws IOException {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize);
    }
    kafka.consumer.SimpleConsumer underlying ;


    /**
     *  Fetch a set of messages from a topic.
     *
     *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     *  @return a set of fetched messages
     */
    public ByteBufferMessageSet fetch(FetchRequest request)throws IOException {
        kafka.message.ByteBufferMessageSet messageSet = underlying.fetch(request);
        return new ByteBufferMessageSet(messageSet.getBuffer(), messageSet.getInitialOffset(),
                messageSet.getErrorCode());
    }

    /**
     *  Combine multiple fetch requests in one call.
     *
     *  @param fetches  a sequence of fetch requests.
     *  @return a sequence of fetch responses
     */
    public MultiFetchResponse multifetch(java.util.List<FetchRequest> fetches) throws IOException{
        FetchRequest[] fetchRequests = new FetchRequest[fetches.size()];
        kafka.api.MultiFetchResponse response = underlying.multifetch(fetches.toArray(fetchRequests));
        return new kafka.javaapi.MultiFetchResponse(response.buffer(), response.numSets(), response.offsets());
    }

    /**
     *  Get a list of valid offsets (up to maxSize) before the given time.
     *  The result is a list of offsets, in descending order.
     *
     *  @param time: time in millisecs (-1, from the latest offset available, -2 from the smallest offset available)
     *  @return an array of offsets
     */
    public long[] getOffsetsBefore(String topic,int  partition,long time, int maxNumOffsets) throws IOException{
        return underlying.getOffsetsBefore(topic, partition, time, maxNumOffsets);
    }


    public void close() throws IOException{
        underlying.close();
    }
}

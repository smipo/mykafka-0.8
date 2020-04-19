package kafka.javaapi.consumer;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;

import java.io.IOException;

public class SimpleConsumer {

    String host;
    int port;
    int soTimeout;
    int bufferSize;
    String clientId;

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.clientId = clientId;

        underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
    }

    kafka.consumer.SimpleConsumer underlying ;


    /**
     *  Fetch a set of messages from a topic. This version of the fetch method
     *  takes the Scala version of a fetch request (i.e.,
     *  [[kafka.api.FetchRequest]] and is intended for use with the
     *  [[kafka.api.FetchRequestBuilder]].
     *
     *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     *  @return a set of fetched messages
     */
    public FetchResponse fetch(kafka.api.FetchRequest request) throws IOException {
        return new FetchResponse(underlying.fetch(request));
    }

    /**
     *  Fetch a set of messages from a topic.
     *
     *  @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     *  @return a set of fetched messages
     */
    public FetchResponse fetch(kafka.javaapi.FetchRequest request) throws IOException {
        return fetch(request.underlying);
    }

    /**
     *  Fetch metadata for a sequence of topics.
     *
     *  @param request specifies the versionId, clientId, sequence of topics.
     *  @return metadata for each topic in the request.
     */
    public kafka.javaapi.TopicMetadataResponse send( kafka.javaapi.TopicMetadataRequest request) throws IOException {
        return new kafka.javaapi.TopicMetadataResponse(underlying.send(request.underlying));
    }

    /**
     *  Get a list of valid offsets (up to maxSize) before the given time.
     *
     *  @param request a [[kafka.javaapi.OffsetRequest]] object.
     *  @return a [[kafka.javaapi.OffsetResponse]] object.
     */
    public kafka.javaapi.OffsetResponse getOffsetsBefore(OffsetRequest request) throws IOException {
        return new kafka.javaapi.OffsetResponse(underlying.getOffsetsBefore(request.underlying));
    }

    public void close() throws IOException{
        underlying.close();
    }
}

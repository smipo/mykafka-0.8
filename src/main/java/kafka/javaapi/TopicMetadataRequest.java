package kafka.javaapi;

import kafka.api.RequestOrResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TopicMetadataRequest  extends RequestOrResponse {

    short versionId;
    String clientId;
    List<String> topics;

    public TopicMetadataRequest(short versionId, int correlationId, String clientId, List<String> topics) {
        super(kafka.api.RequestKeys.MetadataKey,correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.topics = topics;
        underlying = new  kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, topics);
    }
    public kafka.api.TopicMetadataRequest underlying;

    public TopicMetadataRequest(List<String> topics) {
        this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics);
    }
    public TopicMetadataRequest(List<String> topics,int correlationId) {
        this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics);
    }
    public void writeTo(ByteBuffer buffer) throws IOException {
        underlying.writeTo(buffer);
    }

    public int sizeInBytes() throws IOException {
        return underlying.sizeInBytes();
    }
}

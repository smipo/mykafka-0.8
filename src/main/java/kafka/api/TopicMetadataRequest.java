package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TopicMetadataRequest extends RequestOrResponse{

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";

    /**
     * TopicMetadataRequest has the following format -
     * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
     */

    public static TopicMetadataRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int numTopics = ApiUtils.readIntInRange(buffer, "number of topics", 0, Integer.MAX_VALUE);
        List<String> topics = new ArrayList<>();
        for(int i = 0;i < numTopics;i++){
            topics.add(ApiUtils.readShortString(buffer));
        }
        return new TopicMetadataRequest(versionId, correlationId, clientId, topics);
    }
    short versionId;
    String clientId;
    List<String> topics;

    public TopicMetadataRequest(short versionId,int correlationId, String clientId, List<String> topics) {
        super(RequestKeys.MetadataKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.topics = topics;
    }

    public TopicMetadataRequest(List<String> topics, int correlationId) {
        this(TopicMetadataRequest.CurrentVersion, correlationId, TopicMetadataRequest.DefaultClientId, topics);
    }
    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(topics.size());
        for(String topic:topics){
            ApiUtils.writeShortString(buffer, topic);
        }
    }

    public int sizeInBytes() throws IOException{
        int sum = 0;
        for(String topic:topics){
            sum += ApiUtils.shortStringLength(topic);
        }
        return 2 +  /* version id */
                4 + /* correlation id */
                ApiUtils.shortStringLength(clientId)  + /* client id */
                4 + /* number of topics */
                sum;/* topics */
    }
    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) throws IOException,InterruptedException{
        List<TopicMetadata> topicMetadata = topics.stream().map(topic ->  new TopicMetadata(topic,null,ErrorMapping.codeFor(e.getClass().getName())))
                .collect(Collectors.toList());
        TopicMetadataResponse errorResponse = new TopicMetadataResponse(correlationId,topicMetadata);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

}

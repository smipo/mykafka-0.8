package kafka.api;

import kafka.network.RequestChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TopicMetadataRequest extends RequestOrResponse{

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";

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
    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request){

    }

}

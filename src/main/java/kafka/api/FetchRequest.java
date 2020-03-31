package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.network.RequestChannel;
import kafka.utils.Pair;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchRequest extends RequestOrResponse {

    public static short CurrentVersion = 0;
    public static int DefaultMaxWait = 0;
    public static int DefaultMinBytes = 0;
    public static int DefaultCorrelationId = 0;


    short versionId ;
    String clientId ;
    int replicaId ;
    int maxWait;
    int minBytes;
    Map<TopicAndPartition, PartitionFetchInfo> requestInfo;

    public FetchRequest(int correlationId, short versionId, String clientId, int replicaId, int maxWait, int minBytes, Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        super(RequestKeys.FetchKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.requestInfo = requestInfo;

        requestInfo.forEach( (k,v)->{
            List<Pair<TopicAndPartition,PartitionFetchInfo>> list = requestInfoGroupedByTopic.get(k.topic());
            if(list == null){
                list = new ArrayList<>();
                requestInfoGroupedByTopic.put(k.topic(),list);
            }
            list.add(new Pair<>(k,v));
        } );
    }
    Map<String, List<Pair<TopicAndPartition,PartitionFetchInfo>>> requestInfoGroupedByTopic = new HashMap<>();


    public void writeTo(ByteBuffer buffer) throws IOException{
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(replicaId);
        buffer.putInt(maxWait);
        buffer.putInt(minBytes);
        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count
        for (Map.Entry<String, List<Pair<TopicAndPartition,PartitionFetchInfo>>> entry : requestInfoGroupedByTopic.entrySet()) {
            String topic = entry.getKey();
            List<Pair<TopicAndPartition,PartitionFetchInfo>> partitionFetchInfos = entry.getValue();
            ApiUtils.writeShortString(buffer, topic);
            buffer.putInt(partitionFetchInfos.size()) ;// partition count
            for(Pair<TopicAndPartition,PartitionFetchInfo> pair:partitionFetchInfos){
                buffer.putInt(pair.getKey().partition());
                buffer.putLong(pair.getValue().getOffset());
                buffer.putInt(pair.getValue().getFetchSize());
            }
        }
    }

    public  int sizeInBytes() throws IOException{
        int sum = 0;
        for (Map.Entry<String, List<Pair<TopicAndPartition,PartitionFetchInfo>>> entry : requestInfoGroupedByTopic.entrySet()) {
            String topic = entry.getKey();
            List<Pair<TopicAndPartition, PartitionFetchInfo>> partitionFetchInfos = entry.getValue();
            sum += ApiUtils.shortStringLength(topic) + partitionFetchInfos.size() * (
                    4 + /* partition id */
                            8 + /* offset */
                            4 /* fetch size */
            );
        }
        return  2 + /* versionId */
                4 + /* correlationId */
               ApiUtils.shortStringLength(clientId) +
                4 + /* replicaId */
                4 + /* maxWait */
                4 + /* minBytes */
                4 + /* topic count */
                sum;
    }

    public boolean isFromFollower(){
        return replicaId != RequestOrResponse.OrdinaryConsumerId && replicaId != RequestOrResponse.DebuggingConsumerId;
    }

    public boolean isFromOrdinaryConsumer(){
        return replicaId == RequestOrResponse.OrdinaryConsumerId;
    }

    public boolean isFromLowLevelConsumer (){
        return replicaId == RequestOrResponse.DebuggingConsumerId;
    }

    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        val fetchResponsePartitionData = requestInfo.map {
            case (topicAndPartition, data) =>
                (topicAndPartition, FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), -1, MessageSet.Empty))
        }
        val errorResponse = new FetchResponse(correlationId, fetchResponsePartitionData);
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(errorResponse)))
    }

    /**
     *  Public constructor for the clients
     */
    public FetchRequest(int correlationId,
                        String clientId,
                        int maxWait, int minBytes,
                        Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this(correlationId,FetchRequest.CurrentVersion,
                clientId,
                RequestOrResponse.OrdinaryConsumerId,
                maxWait,
                minBytes,
                requestInfo);
    }

    public static class PartitionFetchInfo{
        long offset;
        int fetchSize;

        public long getOffset() {
            return offset;
        }

        public int getFetchSize() {
            return fetchSize;
        }
    }
}

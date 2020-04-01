package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.message.MessageSet;
import kafka.network.RequestChannel;
import kafka.utils.Pair;
import kafka.utils.Utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchRequest extends RequestOrResponse {

    public static short CurrentVersion = 0;
    public static int DefaultMaxWait = 0;
    public static int DefaultMinBytes = 0;
    public static int DefaultCorrelationId = 0;


    public static FetchRequest readFrom(ByteBuffer buffer) throws UnsupportedEncodingException {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int replicaId = buffer.getInt();
        int maxWait = buffer.getInt();
        int minBytes = buffer.getInt();
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, PartitionFetchInfo> requestInfo = new HashMap<>();
        for (int i = 1; i < topicCount; i++) {
            String topic = ApiUtils.readShortString(buffer);
            int partitionCount = buffer.getInt();
            for (int j = 1; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                long offset = buffer.getLong();
                int fetchSize = buffer.getInt();
                requestInfo.put(new TopicAndPartition(topic, partitionId), new PartitionFetchInfo(offset, fetchSize));
            }
        }
        return new FetchRequest(correlationId,versionId, clientId, replicaId, maxWait, minBytes, requestInfo);
    }
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
        Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> data = new HashMap<>();
        for(Map.Entry<TopicAndPartition, PartitionFetchInfo> entry : requestInfo.entrySet()){
            data.put(entry.getKey(),new FetchResponse.FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass().getName()),-1, MessageSet.Empty));
        }
        FetchResponse errorResponse = new FetchResponse(correlationId, data);
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponse.FetchResponseSend(errorResponse)));
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

        public PartitionFetchInfo(long offset, int fetchSize) {
            this.offset = offset;
            this.fetchSize = fetchSize;
        }

        public long getOffset() {
            return offset;
        }

        public int getFetchSize() {
            return fetchSize;
        }
    }

    public static class FetchRequestBuilder{
        private AtomicInteger correlationId = new AtomicInteger(0);
        private short versionId = FetchRequest.CurrentVersion;
        private String clientId = "";
        private int replicaId = RequestOrResponse.OrdinaryConsumerId;
        private int maxWait = FetchRequest.DefaultMaxWait;
        private int minBytes = FetchRequest.DefaultMinBytes;
        private Map<TopicAndPartition, PartitionFetchInfo> requestMap = new HashMap<>();

        public FetchRequestBuilder addFetch(String topic, int partition, long offset, int fetchSize)  {
            requestMap.put(new TopicAndPartition(topic, partition), new PartitionFetchInfo(offset, fetchSize));
            return this;
        }

        public FetchRequestBuilder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Only for internal use. Clients shouldn't set replicaId.
         */
        private FetchRequestBuilder replicaId(int replicaId){
            this.replicaId = replicaId;
            return this;
        }

        public FetchRequestBuilder maxWait(int maxWait) {
            this.maxWait = maxWait;
            return this;
        }

        public FetchRequestBuilder minBytes(int minBytes) {
            this.minBytes = minBytes;
            return this;
        }

        public FetchRequest build()  {
            FetchRequest fetchRequest = new FetchRequest(correlationId.getAndIncrement(),versionId, clientId, replicaId, maxWait, minBytes, requestMap);
            requestMap.clear();
            return fetchRequest;
        }
    }
}

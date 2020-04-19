package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetRequest extends RequestOrResponse {

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;


    public static OffsetRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int replicaId = buffer.getInt();
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        for(int i = 0;i < topicCount;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partitionCount = buffer.getInt();
            for(int j = 0;j < partitionCount;j++){
                int partitionId = buffer.getInt();
                long time = buffer.getLong();
                int maxNumOffsets = buffer.getInt();
                requestInfo.put(new TopicAndPartition(topic, partitionId),new  PartitionOffsetRequestInfo(time, maxNumOffsets));
            }
        }
        return new OffsetRequest(correlationId, requestInfo,versionId,clientId , replicaId);
    }

    public static class PartitionOffsetRequestInfo {
        public long time;
        public int maxNumOffsets;

        public PartitionOffsetRequestInfo(long time, int maxNumOffsets) {
            this.time = time;
            this.maxNumOffsets = maxNumOffsets;
        }
    }

    public Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;
    public short versionId;
    public String clientId;
    public int replicaId;

    public OffsetRequest(int correlationId, Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, short versionId, String clientId, int replicaId) {
        super(RequestKeys.OffsetsKey, correlationId);
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;
        this.replicaId = replicaId;

        for (Map.Entry<TopicAndPartition, PartitionOffsetRequestInfo> entry : requestInfo.entrySet()) {
            List<Pair<TopicAndPartition, PartitionOffsetRequestInfo>> list = requestInfoGroupedByTopic.get(entry.getKey().topic());
            if (list == null) {
                list = new ArrayList<>();
                requestInfoGroupedByTopic.put(entry.getKey().topic(), list);
            }
            list.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
    }

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, int correlationId, int replicaId) {
        this(correlationId, requestInfo, OffsetRequest.CurrentVersion, OffsetRequest.DefaultClientId, replicaId);
    }

    Map<String, List<Pair<TopicAndPartition, PartitionOffsetRequestInfo>>> requestInfoGroupedByTopic = new HashMap<>();

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(replicaId);
        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count

        for (Map.Entry<String, List<Pair<TopicAndPartition, PartitionOffsetRequestInfo>>> entry : requestInfoGroupedByTopic.entrySet()) {
            List<Pair<TopicAndPartition, PartitionOffsetRequestInfo>> partitionInfos = entry.getValue();
            ApiUtils.writeShortString(buffer, entry.getKey());
            buffer.putInt(partitionInfos.size()); // partition count
            for (Pair<TopicAndPartition, PartitionOffsetRequestInfo> pair : partitionInfos) {
                buffer.putInt(pair.getKey().partition());
                buffer.putLong(pair.getValue().time);
                buffer.putInt(pair.getValue().maxNumOffsets);
            }
        }
    }

    public int sizeInBytes() throws IOException {
        int size = 2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.shortStringLength(clientId) +
                4 + /* replicaId */
                4; /* topic count */
        for (Map.Entry<String, List<Pair<TopicAndPartition, PartitionOffsetRequestInfo>>> entry : requestInfoGroupedByTopic.entrySet()) {
            size += ApiUtils.shortStringLength(entry.getKey()) +
                    4 + /* partition count */
                    entry.getValue().size() * (
                            4 + /* partition */
                                    8 + /* time */
                                    4 /* maxNumOffsets */
                    );
        }
        return size;
    }

    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        Map<TopicAndPartition, OffsetResponse.PartitionOffsetsResponse> partitionErrorAndOffsets = new HashMap<>();
        for (Map.Entry<TopicAndPartition, PartitionOffsetRequestInfo> entry : requestInfo.entrySet()) {
            partitionErrorAndOffsets.put(entry.getKey(),new OffsetResponse.PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass().getName()), null));
        }
        OffsetResponse errorResponse = new OffsetResponse(correlationId, partitionErrorAndOffsets);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
    public boolean isFromOrdinaryClient (){
        return replicaId == RequestOrResponse.OrdinaryConsumerId;
    }
    public boolean isFromDebuggingClient(){
        return replicaId == RequestOrResponse.DebuggingConsumerId;
    }
    @Override
    public String toString() {
        return "OffsetRequest{" +
                "requestInfo=" + requestInfo +
                ", versionId=" + versionId +
                ", clientId='" + clientId + '\'' +
                ", replicaId=" + replicaId +
                ", requestInfoGroupedByTopic=" + requestInfoGroupedByTopic +
                '}';
    }
}

package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UpdateMetadataRequest extends RequestOrResponse {


    public static UpdateMetadataRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();
        Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo>  partitionStateInfos = new HashMap<>();

        for(int i = 0 ;i < partitionStateInfosCount;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partition = buffer.getInt();
            LeaderAndIsrRequest.PartitionStateInfo partitionStateInfo = LeaderAndIsrRequest.PartitionStateInfo.readFrom(buffer);

            partitionStateInfos.put(new TopicAndPartition(topic, partition), partitionStateInfo);
        }

        int numAliveBrokers = buffer.getInt();
        Set<Broker> aliveBrokers = new HashSet<>();
        for(int i = 0 ;i < numAliveBrokers;i++){
            aliveBrokers.add(Broker.readFrom(buffer));
        }
        return new UpdateMetadataRequest(correlationId,versionId, clientId, controllerId, controllerEpoch,
                partitionStateInfos, aliveBrokers);
    }

    public static short CurrentVersion = 0;
    public static boolean IsInit = true;
    public static boolean NotInit = false;
    public static int DefaultAckTimeout = 1000;


    public short versionId;
    public String clientId;
    public int controllerId;
    public  int controllerEpoch;
    public  Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos;
    public Set<Broker> aliveBrokers;

    public UpdateMetadataRequest(int correlationId, short versionId, String clientId, int controllerId, int controllerEpoch, Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos, Set<Broker> aliveBrokers) {
        super(RequestKeys.UpdateMetadataKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.aliveBrokers = aliveBrokers;
    }

    public UpdateMetadataRequest(int controllerId, int controllerEpoch, int correlationId, String clientId,
                                 Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos, Set<Broker> aliveBrokers) {
        this(correlationId, UpdateMetadataRequest.CurrentVersion, clientId,
                controllerId, controllerEpoch, partitionStateInfos, aliveBrokers);
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.putInt(partitionStateInfos.size());
        for (Map.Entry<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> entry : partitionStateInfos.entrySet()) {
            ApiUtils.writeShortString(buffer, entry.getKey().topic());
            buffer.putInt(entry.getKey().partition());
            entry.getValue().writeTo(buffer);
        }
        buffer.putInt(aliveBrokers.size());
        for (Broker b : aliveBrokers) {
            b.writeTo(buffer);
        }
    }

    public int sizeInBytes() throws IOException {
        int size =
                2 /* version id */ +
                        4 /* correlation id */ +
                        (2 + clientId.length()) /* client id */ +
                        4 /* controller id */ +
                        4 /* controller epoch */ +
                        4; /* number of partitions */
        for (Map.Entry<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> entry : partitionStateInfos.entrySet()) {
            size += (2 + entry.getKey().topic().length()) /* topic */ + 4 /* partition */ + entry.getValue().sizeInBytes(); /* partition state info */
        }
        size += 4;/* number of alive brokers in the cluster */
        for (Broker b : aliveBrokers) {
            size += b.sizeInBytes(); /* broker info */
        }
        return size;
    }

    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        UpdateMetadataResponse errorResponse = new UpdateMetadataResponse(correlationId, ErrorMapping.codeFor(e.getCause().getClass().getName()));
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}

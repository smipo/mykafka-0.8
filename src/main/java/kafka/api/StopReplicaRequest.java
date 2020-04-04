package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.InvalidRequestException;
import kafka.network.RequestChannel;
import kafka.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StopReplicaRequest  extends RequestOrResponse{

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";
    public static int DefaultAckTimeout = 100;

    public static StopReplicaRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        byte d = buffer.get();
        boolean deletePartitions ;
        if(d == 1){
            deletePartitions = true;
        }else if(d == 0){
            deletePartitions = false;
        }else{
            throw new InvalidRequestException("Invalid byte %d in delete partitions field. (Assuming false.)".format(d + ""));
        }
        int topicPartitionPairCount = buffer.getInt();
        Set<Pair<String, Integer>> topicPartitionPairSet = new HashSet<>();
        for(int i = 1;i < topicPartitionPairCount;i++){
            topicPartitionPairSet.add(new Pair<>(ApiUtils.readShortString(buffer), buffer.getInt()));
        }
        return new StopReplicaRequest(correlationId,versionId, clientId, controllerId, controllerEpoch,
                deletePartitions, topicPartitionPairSet);
    }

    public short versionId;
    public String clientId;
    public int controllerId;
    public int controllerEpoch;
    public boolean deletePartitions;
    public Set<Pair<String, Integer>> partitions;

    public StopReplicaRequest(int correlationId,short versionId, String clientId, int controllerId, int controllerEpoch, boolean deletePartitions, Set<Pair<String, Integer>> partitions) {
        super(RequestKeys.StopReplicaKey,correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest(boolean deletePartitions, Set<Pair<String, Integer>> partitions,int controllerId, int controllerEpoch, int correlationId)  {
        this(correlationId,StopReplicaRequest.CurrentVersion, StopReplicaRequest.DefaultClientId,
                controllerId, controllerEpoch, deletePartitions, partitions);
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.put(deletePartitions == true? "1".getBytes() : "0".getBytes());
        buffer.putInt(partitions.size());
        for (Pair<String, Integer> pair : partitions){
            ApiUtils.writeShortString(buffer, pair.getKey());
            buffer.putInt(pair.getValue());
        }
    }

    public int sizeInBytes() throws IOException {
        int size =
                2 + /* versionId */
                        4 + /* correlation id */
                        ApiUtils.shortStringLength(clientId) +
                        4 + /* controller id*/
                        4 + /* controller epoch */
                        1 + /* deletePartitions */
                        4; /* partition count */
        for (Pair<String, Integer> pair : partitions){
            size += (ApiUtils.shortStringLength(pair.getKey())) +
                    4 ;/* partition id */
        }
        return size;
    }
    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        Map<Pair<String, Integer>, Short> responseMap = new HashMap<>();
        for (Pair<String, Integer> pair : partitions){
            responseMap.put(pair, ErrorMapping.codeFor(e.getClass().getName()));
        }
        StopReplicaResponse errorResponse = new StopReplicaResponse(correlationId, responseMap, ErrorMapping.NoError);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}

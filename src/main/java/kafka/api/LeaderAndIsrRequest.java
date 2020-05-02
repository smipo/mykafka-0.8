package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class LeaderAndIsrRequest extends RequestOrResponse{

    public static short CurrentVersion = 0;
    public static boolean IsInit = true;
    public static boolean NotInit = false;
    public static int DefaultAckTimeout = 1000;


    public static LeaderAndIsrRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();
        Map<Pair<String, Integer>, PartitionStateInfo> partitionStateInfos = new HashMap<>();

        for(int i = 0 ;i < partitionStateInfosCount;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partition = buffer.getInt();
            PartitionStateInfo partitionStateInfo = PartitionStateInfo.readFrom(buffer);

            partitionStateInfos.put(new Pair(topic, partition), partitionStateInfo);
        }
        int leadersCount = buffer.getInt();
        Set<Broker> leaders = new HashSet<>();
        for(int i = 0 ;i < leadersCount;i++){
            leaders.add(Broker.readFrom(buffer));
        }
        return new LeaderAndIsrRequest(correlationId,versionId, clientId, controllerId, controllerEpoch, partitionStateInfos, leaders);
    }

    public short versionId;
    public String clientId;
    public int controllerId;
    public  int controllerEpoch;
    public  Map<Pair<String, Integer>, PartitionStateInfo> partitionStateInfos;
    public  Set<Broker> leaders;

    public LeaderAndIsrRequest(int correlationId,short versionId, String clientId, int controllerId, int controllerEpoch, Map<Pair<String, Integer>, PartitionStateInfo> partitionStateInfos, Set<Broker> leaders) {
        super(RequestKeys.LeaderAndIsrKey,correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.leaders = leaders;
    }

    public LeaderAndIsrRequest( Map<Pair<String, Integer>, PartitionStateInfo> partitionStateInfos,Set<Broker> leaders,int controllerId,
        int controllerEpoch, int correlationId,String clientId)  {
        this(correlationId,LeaderAndIsrRequest.CurrentVersion, clientId,
                controllerId, controllerEpoch, partitionStateInfos, leaders);
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.putInt(partitionStateInfos.size());
        for (Map.Entry<Pair<String, Integer>, PartitionStateInfo> entry : partitionStateInfos.entrySet()) {
            ApiUtils.writeShortString(buffer, entry.getKey().getKey());
            buffer.putInt(entry.getKey().getValue());
            entry.getValue().writeTo(buffer);
        }
        buffer.putInt(leaders.size());
        for(Broker b:leaders){
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
        for (Map.Entry<Pair<String, Integer>, PartitionStateInfo> entry : partitionStateInfos.entrySet()) {
            size += (2 + entry.getKey().getKey().length()) /* topic */ + 4 /* partition */ + entry.getValue().sizeInBytes(); /* partition state info */
        }
        size += 4;/* number of leader brokers */
        for (Broker broker : leaders) {
            size += broker.sizeInBytes(); /* broker info */
        }
        return size;
    }
    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        Map<Pair<String, Integer>, Short> responseMap = new HashMap<>();
        for (Map.Entry<Pair<String, Integer>, PartitionStateInfo> entry : partitionStateInfos.entrySet()) {
            responseMap.put(entry.getKey(),ErrorMapping.codeFor(e.getClass().getName()));
        }
        LeaderAndIsrResponse errorResponse = new LeaderAndIsrResponse(correlationId, responseMap, ErrorMapping.NoError);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
    public static class LeaderAndIsr {
        public static int initialLeaderEpoch = 0;
        public static int initialZKVersion = 0;

        public int leader;
        public int leaderEpoch;
        public List<Integer> isr;
        public int zkVersion;

        public LeaderAndIsr(int leader, int leaderEpoch, List<Integer> isr, int zkVersion) {
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.isr = isr;
            this.zkVersion = zkVersion;
        }
        public LeaderAndIsr(int leader,List<Integer> isr) {
            this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion);
        }

        @Override
        public String toString() {
            return "LeaderAndIsr{" +
                    "leader=" + leader +
                    ", leaderEpoch=" + leaderEpoch +
                    ", isr=" + isr +
                    ", zkVersion=" + zkVersion +
                    '}';
        }
    }

    public static class PartitionStateInfo{

        public static PartitionStateInfo  readFrom(ByteBuffer buffer) {
            int controllerEpoch = buffer.getInt();
            int leader = buffer.getInt();
            int leaderEpoch = buffer.getInt();
            int isrSize = buffer.getInt();
            List<Integer> isr = new ArrayList<>();
            for(int i = 0;i < isrSize;i++){
                isr.add(buffer.getInt());
            }
            int zkVersion = buffer.getInt();
            int replicationFactor = buffer.getInt();
            Set<Integer> allReplicas = new HashSet<>();
            for(int i = 0;i < replicationFactor;i++){
                allReplicas.add(buffer.getInt());
            }
           return new PartitionStateInfo(new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, leaderEpoch, isr, zkVersion), controllerEpoch),
                   allReplicas);
        }

        public LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch;
        public Set<Integer> allReplicas;

        public PartitionStateInfo(LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch, Set<Integer> allReplicas) {
            this.leaderIsrAndControllerEpoch = leaderIsrAndControllerEpoch;
            this.allReplicas = allReplicas;
        }

        public int replicationFactor(){
            return allReplicas.size();
        }

        public void writeTo(ByteBuffer buffer) throws IOException {
            buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch);
            buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader);
            buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch);
            buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size());
            leaderIsrAndControllerEpoch.leaderAndIsr.isr.forEach(i ->buffer.putInt(i));
            buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion);
            buffer.putInt(replicationFactor());
            allReplicas.forEach(i->buffer.putInt(i));
        }

        public int sizeInBytes() throws IOException {
            int size =
                    4 /* epoch of the controller that elected the leader */ +
                            4 /* leader broker id */ +
                            4 /* leader epoch */ +
                            4 /* number of replicas in isr */ +
                            4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size() /* replicas in isr */ +
                            4 /* zk version */ +
                            4 /* replication factor */ +
                            allReplicas.size() * 4;
            return size;
        }
    }

}

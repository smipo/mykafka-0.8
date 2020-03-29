package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopicMetadata {

    public static int NoLeaderNodeId = -1;

    public static TopicMetadata readFrom(ByteBuffer buffer,Map<Integer, Broker> brokers) throws UnsupportedEncodingException{
        short errorCode =  ApiUtils.readShortInRange(buffer, "error code", (short) -1, Short.MAX_VALUE);
        String topic =  ApiUtils.readShortString(buffer);
        int numPartitions = ApiUtils.readIntInRange(buffer, "number of partitions",0, Integer.MAX_VALUE);
        List<PartitionMetadata> partitionsMetadata = new ArrayList<>();
        for(int i = 0;i < numPartitions;i++)
            partitionsMetadata .add(PartitionMetadata.readFrom(buffer, brokers));
        return new TopicMetadata(topic, partitionsMetadata, errorCode);
    }

    String topic;
    List<PartitionMetadata> partitionsMetadata;
    short errorCode = ErrorMapping.NoError;

    public TopicMetadata(String topic,List<PartitionMetadata> partitionsMetadata, short errorCode) {
        this.topic = topic;
        this.partitionsMetadata = partitionsMetadata;
        this.errorCode = errorCode;
    }

    public int sizeInBytes() throws UnsupportedEncodingException {
        return 2 /* error code */ +
                ApiUtils.shortStringLength(topic) +
                4 + partitionsMetadata.stream().mapToInt(PartitionMetadata::sizeInBytes).sum(); /* size and partition data array */
    }

    public void writeTo(ByteBuffer buffer) throws UnsupportedEncodingException{
        /* error code */
        buffer.putShort(errorCode);
        /* topic */
        ApiUtils.writeShortString(buffer, topic);
        /* number of partitions */
        buffer.putInt(partitionsMetadata.size());
        partitionsMetadata.forEach(m -> m.writeTo(buffer));
    }




   public static class PartitionMetadata{

       public static PartitionMetadata readFrom(ByteBuffer buffer, Map<Integer, Broker> brokers) {
           short errorCode = ApiUtils.readShortInRange(buffer, "error code", (short) -1, Short.MAX_VALUE);
           int partitionId = ApiUtils.readIntInRange(buffer, "partition id", 0, Integer.MAX_VALUE); /* partition id */
           int leaderId = buffer.getInt();
           Broker leader = brokers.get(leaderId);

           /* list of all replicas */
           int numReplicas = ApiUtils.readIntInRange(buffer, "number of all replicas", 0, Integer.MAX_VALUE);
           List<Integer> replicaIds = new ArrayList<>();
           for(int i = 0;i < numReplicas;i++){
               replicaIds.add(buffer.getInt());
           }
           List<Broker> replicas = new ArrayList<>();
           for(Integer replicaId:replicaIds){
               replicas.add(brokers.get(replicaId));
           }
           /* list of in-sync replicas */
           int numIsr = ApiUtils.readIntInRange(buffer, "number of in-sync replicas",0, Integer.MAX_VALUE);
           List<Integer> isrIds = new ArrayList<>();
           for(int i = 0;i < numIsr;i++){
               isrIds.add(buffer.getInt());
           }
           List<Broker> isr = new ArrayList<>();
           for(Integer isrId:isrIds){
               isr.add(brokers.get(isrId));
           }
           return new PartitionMetadata(partitionId, leader, replicas, isr, errorCode);
       }

       int partitionId;
       Broker leader;
       List<Broker> replicas;
       List<Broker> isr;
       short errorCode = ErrorMapping.NoError;

       public PartitionMetadata(int partitionId, Broker leader, List<Broker> replicas, List<Broker> isr, short errorCode) {
           this.partitionId = partitionId;
           this.leader = leader;
           this.replicas = replicas;
           this.isr = isr;
           this.errorCode = errorCode;
       }

       public int sizeInBytes(){
           return 2 /* error code */ +
                   4 /* partition id */ +
                   4 /* leader */ +
                   4 + 4 * replicas.size() /* replica array */ +
                   4 + 4 * isr.size() ;/* isr array */
       }

       public void writeTo(ByteBuffer buffer) {
           buffer.putShort(errorCode);
           buffer.putInt(partitionId);

           /* leader */
           int leaderId = leader == null ? TopicMetadata.NoLeaderNodeId : leader.id();
           buffer.putInt(leaderId);

           /* number of replicas */
           buffer.putInt(replicas.size());
           replicas.forEach(r -> buffer.putInt(r.id()));

           /* number of in-sync replicas */
           buffer.putInt(isr.size());
           isr.forEach(r -> buffer.putInt(r.id()));
       }
   }
}

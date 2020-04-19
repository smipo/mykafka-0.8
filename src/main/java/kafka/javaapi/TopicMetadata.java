package kafka.javaapi;

import kafka.cluster.Broker;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class TopicMetadata {

    kafka.api.TopicMetadata underlying;

    public TopicMetadata(kafka.api.TopicMetadata underlying) {
        this.underlying = underlying;
    }

    public String topic(){
        return underlying.topic;
    }

    public List<PartitionMetadata> partitionsMetadata() {
        List<PartitionMetadata> list = new ArrayList<>();
         for(kafka.api.TopicMetadata.PartitionMetadata p:underlying.partitionsMetadata){
             list.add(new PartitionMetadata(p));
         }
         return list;
    }

    public short errorCode(){
        return underlying.errorCode;
    }

    public int sizeInBytes() throws UnsupportedEncodingException {
        return underlying.sizeInBytes();
    }

    public static class PartitionMetadata{

        kafka.api.TopicMetadata.PartitionMetadata underlying;

        public PartitionMetadata(kafka.api.TopicMetadata.PartitionMetadata underlying) {
            this.underlying = underlying;
        }

        public int partitionId(){
            return underlying.partitionId;
        }

        public Broker leader() {
            return underlying.leader;
        }

        public List<Broker> replicas() {
            return underlying.replicas;
        }

        public List<Broker> isr() {
            return underlying.isr;
        }

        public short errorCode(){
            return underlying.errorCode;
        }

        public int sizeInBytes(){
            return underlying.sizeInBytes();
        }
    }


}

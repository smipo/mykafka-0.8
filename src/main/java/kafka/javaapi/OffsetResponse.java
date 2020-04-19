package kafka.javaapi;

import kafka.common.TopicAndPartition;

import java.util.List;

public class OffsetResponse {

    kafka.api.OffsetResponse underlying;

    public OffsetResponse(kafka.api.OffsetResponse underlying) {
        this.underlying = underlying;
    }

    public boolean hasError (){
        return underlying.hasError();
    }


    public int errorCode(String topic, int partition) {
        return underlying.partitionErrorAndOffsets.get(new TopicAndPartition(topic, partition)).error;
    }


    public List<Long> offsets(String topic, int partition) {
        return underlying.partitionErrorAndOffsets.get(new TopicAndPartition(topic, partition)).offsets;
    }




}

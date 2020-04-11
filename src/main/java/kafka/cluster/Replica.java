package kafka.cluster;

import kafka.common.KafkaException;
import kafka.log.Log;
import kafka.server.ReplicaManager;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class Replica {

    private static Logger logger = Logger.getLogger(Replica.class);

    public int brokerId;
    public Partition partition;
    public long milliseconds;
    public long initialHighWatermarkValue;
    public Log log;

    public Replica(int brokerId, Partition partition, long milliseconds, long initialHighWatermarkValue, Log log) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.milliseconds = milliseconds;
        this.initialHighWatermarkValue = initialHighWatermarkValue;
        this.log = log;

        highWatermarkValue = new AtomicLong(initialHighWatermarkValue);
        logEndOffsetValue = new AtomicLong(ReplicaManager.UnknownLogEndOffset);
        logEndOffsetUpdateTimeMsValue = new AtomicLong(milliseconds);
        topic = partition.topic;
        partitionId = partition.partitionId;
    }
    //only defined in local replica
    public  AtomicLong highWatermarkValue;
    // only used for remote replica; logEndOffsetValue for local replica is kept in log
    public  AtomicLong logEndOffsetValue ;
    public  AtomicLong logEndOffsetUpdateTimeMsValue ;
    public  String topic ;
    public int partitionId ;
    public void logEndOffset(long newLogEndOffset) {
        if (!isLocal()) {
            logEndOffsetValue.set(newLogEndOffset);
            logEndOffsetUpdateTimeMsValue.set(milliseconds);
            logger.trace("Setting log end offset for replica %d for partition [%s,%d] to %d"
                    .format(brokerId+"", topic, partitionId, logEndOffsetValue.get()));
        } else
            throw new KafkaException("Shouldn't set logEndOffset for replica %d partition [%s,%d] since it's local"
                    .format(brokerId+"", topic, partitionId));

    }
    public long logEndOffset() {
        if (isLocal())
            return log.logEndOffset();
        else
            return logEndOffsetValue.get();
    }

    public boolean isLocal() {
        if(log == null){
            return false;
        }
        return true;
    }
    public long logEndOffsetUpdateTimeMs (){
        return logEndOffsetUpdateTimeMsValue.get();
    }

    public void highWatermark_(long newHighWatermark) {
        if (isLocal()) {
            logger.trace("Setting hw for replica %d partition [%s,%d] on broker %d to %d"
                    .format(brokerId+"", topic, partitionId, brokerId, newHighWatermark));
            highWatermarkValue.set(newHighWatermark);
        } else
            throw new KafkaException("Unable to set highwatermark for replica %d partition [%s,%d] since it's not local"
                    .format(brokerId+"", topic, partitionId));
    }

    public long highWatermark(){
        if (isLocal())
            return highWatermarkValue.get();
        else
            throw new KafkaException("Unable to get highwatermark for replica %d partition [%s,%d] since it's not local"
                    .format(brokerId+"", topic, partitionId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica other = (Replica) o;
        return (topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition));
    }

    @Override
    public int hashCode() {
        return 31 + topic.hashCode() + 17*brokerId + partition.hashCode();
    }

    @Override
    public String toString() {
        return "Replica{" +
                "brokerId=" + brokerId +
                ", partition=" + partition +
                ", milliseconds=" + milliseconds +
                ", initialHighWatermarkValue=" + initialHighWatermarkValue +
                '}';
    }
}

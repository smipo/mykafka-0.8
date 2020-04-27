package kafka.server;

import kafka.api.FetchResponse;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.Replica;
import kafka.common.KafkaStorageException;
import kafka.common.TopicAndPartition;
import kafka.log.Log;
import kafka.message.ByteBufferMessageSet;
import org.apache.log4j.Logger;

public class ReplicaFetcherThread  extends AbstractFetcherThread{

    private static Logger logger = Logger.getLogger(ReplicaFetcherThread.class);

    public String name;
    public Broker sourceBroker;
    public KafkaConfig brokerConfig;
    public ReplicaManager replicaMgr;

    public ReplicaFetcherThread(String name, Broker sourceBroker, KafkaConfig brokerConfig, ReplicaManager replicaMgr) {
        super( name,
                name,
                 sourceBroker,
                 brokerConfig.replicaSocketTimeoutMs,
                 brokerConfig.replicaSocketReceiveBufferBytes,
                 brokerConfig.replicaFetchMaxBytes,
                brokerConfig.brokerId,
                 brokerConfig.replicaFetchWaitMaxMs,
                brokerConfig.replicaFetchMinBytes,
               false);
        this.name = name;
        this.sourceBroker = sourceBroker;
        this.brokerConfig = brokerConfig;
        this.replicaMgr = replicaMgr;
    }

    // process fetched data
   public void processPartitionData(TopicAndPartition topicAndPartition, long fetchOffset,
                              FetchResponse.FetchResponsePartitionData partitionData){
        try {
            String topic = topicAndPartition.topic();
            int partitionId = topicAndPartition.partition();
            Replica replica = replicaMgr.getReplica(topic, partitionId);
            ByteBufferMessageSet messageSet = (ByteBufferMessageSet)partitionData.messages;

            if (fetchOffset != replica.logEndOffset())
                throw new RuntimeException(String.format("Offset mismatch: fetched offset = %d, log end offset = %d.",fetchOffset, replica.logEndOffset()));
            logger.trace(String
                    .format("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d",replica.brokerId, replica.logEndOffset(), topicAndPartition, messageSet.sizeInBytes(), partitionData.hw));
            replica.log.append(messageSet,  false);
            logger. trace(String
                    .format("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s",replica.brokerId, replica.logEndOffset(), messageSet.sizeInBytes(), topicAndPartition));
            long followerHighWatermark = Math.min(replica.logEndOffset(),partitionData.hw);
            replica.highWatermark_(followerHighWatermark) ;
            logger. trace(String
                    .format("Follower %d set replica highwatermark for partition [%s,%d] to %d",replica.brokerId, topic, partitionId, followerHighWatermark));
        } catch (KafkaStorageException e){
                logger.fatal("Disk error while replicating data.", e);
                Runtime.getRuntime().halt(1);
        }
    }

    /**
     * Handle a partition whose offset is out of range and return a new fetch offset.
     */
    public long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) throws Throwable {
        Replica replica = replicaMgr.getReplica(topicAndPartition.topic(), topicAndPartition.partition());
        Log log = replica.log;

        /**
         * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
         * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
         * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
         * and it may discover that the current leader's end offset is behind its own end offset.
         *
         * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
         *
         * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
         */
        long leaderEndOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.LatestTime, brokerConfig.brokerId);
        if (leaderEndOffset < log.logEndOffset()) {
            log.truncateTo(leaderEndOffset);
            logger.warn(String
                    .format("Replica %d for partition %s reset its fetch offset to current leader %d's latest offset %d",brokerConfig.brokerId, topicAndPartition, sourceBroker.id(), leaderEndOffset));
            return leaderEndOffset;
        } else {
            /**
             * The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
             * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
             *
             * Roll out a new log at the follower with the start offset equal to the current leader's start offset and continue fetching.
             */
            long leaderStartOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, brokerConfig.brokerId);
            log.truncateAndStartWithNewOffset(leaderStartOffset);
            logger.warn(String
                    .format("Replica %d for partition %s reset its fetch offset to current leader %d's start offset %d",brokerConfig.brokerId, topicAndPartition, sourceBroker.id(), leaderStartOffset));
            return leaderStartOffset;
        }
    }

    // any logic for partitions whose leader has changed
    public void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions) {
        // no handler needed since the controller will make the changes accordingly
    }
}

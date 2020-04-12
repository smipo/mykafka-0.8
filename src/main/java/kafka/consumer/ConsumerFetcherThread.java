package kafka.consumer;

import kafka.api.FetchResponse;
import kafka.api.OffsetRequest;
import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.server.AbstractFetcherThread;

import java.util.Map;

public class ConsumerFetcherThread extends AbstractFetcherThread {

    ConsumerConfig config;
    Broker sourceBroker;
    Map<TopicAndPartition, PartitionTopicInfo> partitionMap;
    ConsumerFetcherManager consumerFetcherManager;

    public ConsumerFetcherThread(String name, ConsumerConfig config, Broker sourceBroker, Map<TopicAndPartition, PartitionTopicInfo> partitionMap, ConsumerFetcherManager consumerFetcherManager) {
        super(name, config.clientId + "-" + name, sourceBroker, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.fetchMessageMaxBytes, RequestOrResponse.OrdinaryConsumerId, config.fetchWaitMaxMs, config.fetchMinBytes, true);
        this.config = config;
        this.sourceBroker = sourceBroker;
        this.partitionMap = partitionMap;
        this.consumerFetcherManager = consumerFetcherManager;
    }

    // process fetched data
    public void processPartitionData(TopicAndPartition topicAndPartition, long fetchOffset,
                              FetchResponse.FetchResponsePartitionData partitionData) throws InterruptedException {
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        if (pti.getFetchOffset() != fetchOffset)
            throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
                    .format(topicAndPartition.topic(), topicAndPartition.partition(), pti.getFetchOffset(), fetchOffset));
        pti.enqueue((ByteBufferMessageSet)partitionData.messages);
    }

    // handle a partition whose offset is out of range and return a new fetch offset
    public long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) throws Throwable {
        long startTimestamp  = 0;
        if(config.autoOffsetReset.equals(OffsetRequest.SmallestTimeString)){
            startTimestamp = OffsetRequest.EarliestTime;
        }else if(config.autoOffsetReset.equals(OffsetRequest.LargestTimeString)){
            startTimestamp = OffsetRequest.LatestTime;
        }else{
            startTimestamp = OffsetRequest.LatestTime;
        }
        long newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, RequestOrResponse.OrdinaryConsumerId);
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        pti.resetFetchOffset(newOffset);
        pti.resetConsumeOffset(newOffset);
        return newOffset;
    }

    // any logic for partitions whose leader has changed
    public void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions) throws InterruptedException {
        while (partitions.iterator().hasNext()){
            TopicAndPartition tap = partitions.iterator().next();
            removePartition(tap.topic(), tap.partition());
        }
        consumerFetcherManager.addPartitionsWithError(partitions);
    }
}

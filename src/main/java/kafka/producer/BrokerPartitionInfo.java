package kafka.producer;

import kafka.api.TopicMetadata;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Partition;

import java.util.*;

public class BrokerPartitionInfo {

    ProducerConfig producerConfig;
    ProducerPool producerPool;
    Map<String, TopicMetadata> topicPartitionInfo;

    public BrokerPartitionInfo(ProducerConfig producerConfig, ProducerPool producerPool, Map<String, TopicMetadata> topicPartitionInfo) {
        this.producerConfig = producerConfig;
        this.producerPool = producerPool;
        this.topicPartitionInfo = topicPartitionInfo;
        brokerList = producerConfig.brokerList;
        brokers = ClientUtils.parseBrokerList(brokerList);
    }

    String brokerList ;
    List<Broker> brokers ;
}

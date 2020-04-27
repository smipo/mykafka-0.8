package kafka.producer;

import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataResponse;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import org.apache.log4j.Logger;

import java.util.*;

public class BrokerPartitionInfo {

    private static Logger logger = Logger.getLogger(BrokerPartitionInfo.class);

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


    /**
     * Return a sequence of (brokerId, numPartitions).
     * @param topic the topic for which this information is to be returned
     * @return a sequence of (brokerId, numPartitions). Returns a zero-length
     * sequence if no brokers are available.
     */
    public List<PartitionAndLeader> getBrokerPartitionInfo(String topic, int correlationId) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        logger.debug(String.format("Getting broker partition info for topic %s",topic));
        // check if the cache has metadata for this topic
        TopicMetadata topicMetadata = topicPartitionInfo.get(topic);
        TopicMetadata metadata = topicMetadata;
        if(metadata == null){
            Set<String> set = new HashSet<>();
            set.add(topic);
            updateInfo(set, correlationId);
            TopicMetadata topicMetadata2 = topicPartitionInfo.get(topic);
            if(topicMetadata2 == null){
                throw new KafkaException("Failed to fetch topic metadata for topic: " + topic);
            }
            metadata = topicMetadata2;
        }
        List<TopicMetadata.PartitionMetadata> partitionMetadata = metadata.partitionsMetadata;
        if(partitionMetadata.size() == 0) {
            if(metadata.errorCode != ErrorMapping.NoError) {
                throw new KafkaException(ErrorMapping.exceptionFor(metadata.errorCode) == null?"":ErrorMapping.exceptionFor(metadata.errorCode).getMessage());
            } else {
                throw new KafkaException(String.format("Topic metadata %s has empty partition metadata and no error code",metadata.toString()));
            }
        }
        List<PartitionAndLeader> res = new ArrayList<>();
        for(TopicMetadata.PartitionMetadata m : partitionMetadata){
            if(m.getLeader() == null){
                logger.debug(String.format("Partition [%s,%d] does not have a leader yet",topic, m.partitionId));
                res.add(new PartitionAndLeader(topic, m.partitionId, null));
            }else{
                logger.debug(String.format("Partition [%s,%d] has leader %d",topic, m.partitionId, m.getLeader().id()));
                res.add(new PartitionAndLeader(topic, m.partitionId, m.getLeader().id()));
            }
        }
        res.sort(Comparator.comparingInt(s -> s.partitionId));
        return res;
    }

    /**
     * It updates the cache by issuing a get topic metadata request to a random broker.
     * @param topics the topics for which the metadata is to be fetched
     */
    public void updateInfo(Set<String> topics, int correlationId) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        TopicMetadataResponse topicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, producerConfig, correlationId);
        List<TopicMetadata> topicsMetadata = topicMetadataResponse.topicsMetadata;
        for(TopicMetadata tmd:topicsMetadata){
            logger.trace(String.format("Metadata for topic %s is %s",tmd.topic, tmd));
            if(tmd.errorCode == ErrorMapping.NoError) {
                topicPartitionInfo.put(tmd.topic, tmd);
            } else
                logger.warn(String.format("Error while fetching metadata [%s] for topic [%s]: %s ",tmd.toString(), tmd.topic, ErrorMapping.exceptionFor(tmd.errorCode).getClass().getName()));
            for(TopicMetadata.PartitionMetadata pmd:tmd.partitionsMetadata){
                if (pmd.errorCode != ErrorMapping.NoError && pmd.errorCode == ErrorMapping.LeaderNotAvailableCode) {
                    logger.warn(String.format("Error while fetching metadata %s for topic partition [%s,%d]: [%s]",pmd.toString(), tmd.topic, pmd.partitionId,
                            ErrorMapping.exceptionFor(pmd.errorCode).getClass().getName()));
                } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
            }
        }
        producerPool.updateProducer(topicsMetadata);
    }


   public static class PartitionAndLeader{
       public  String topic;
       public  int partitionId;
       public  Integer leaderBrokerIdOpt;

       public PartitionAndLeader(String topic, int partitionId, Integer leaderBrokerIdOpt) {
           this.topic = topic;
           this.partitionId = partitionId;
           this.leaderBrokerIdOpt = leaderBrokerIdOpt;
       }
   }
}

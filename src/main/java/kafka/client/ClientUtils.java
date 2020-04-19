package kafka.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.producer.SyncProducer;
import kafka.utils.JacksonUtils;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;


public class ClientUtils {

    private static Logger logger = Logger.getLogger(ClientUtils.class);
    /**
     * Used by the producer to send a metadata request since it has access to the ProducerConfig
     * @param topics The topics for which the metadata needs to be fetched
     * @param brokers The brokers in the cluster as configured on the producer through metadata.broker.list
     * @param producerConfig The producer's config
     * @return topic metadata response
     */
    public static TopicMetadataResponse fetchTopicMetadata(Set<String> topics, List<Broker> brokers,ProducerConfig producerConfig,int correlationId) {
        boolean fetchMetaDataSucceeded = false;
        int i = 0;
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, producerConfig.clientId, topics.stream().collect(Collectors.toList()));
        TopicMetadataResponse topicMetadataResponse = null;
        Throwable t = null;
        // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
        // same broker
        Collections.shuffle(brokers);
        List<Broker> shuffledBrokers = brokers;
        while(i < shuffledBrokers.size() && !fetchMetaDataSucceeded) {
            SyncProducer producer = ProducerPool.createSyncProducer(producerConfig, shuffledBrokers.get(i));
            logger.info("Fetching metadata from broker %s with correlation id %d for %d topic(s) %s".format(shuffledBrokers.get(i).toString(), correlationId, topics.size(), topics));
            try {
                topicMetadataResponse = producer.send(topicMetadataRequest);
                fetchMetaDataSucceeded = true;
            }catch(Throwable e){
                logger.warn("Fetching topic metadata with correlation id %d for topics [%s] from broker [%s] failed"
                            .format(correlationId+"", topics, shuffledBrokers.get(i).toString()), e);
                    t = e;
            } finally {
                i = i + 1;
                producer.close();
            }
        }
        if(!fetchMetaDataSucceeded) {
            throw new KafkaException("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics.toString(), shuffledBrokers.toString()), t);
        } else {
            logger.debug("Successfully fetched metadata for %d topic(s) %s".format(topics.size()+"", topics));
        }
        return topicMetadataResponse;
    }

    /**
     * Used by a non-producer client to send a metadata request
     * @param topics The topics for which the metadata needs to be fetched
     * @param brokers The brokers in the cluster as configured on the client
     * @param clientId The client's identifier
     * @return topic metadata response
     */
    public  static  TopicMetadataResponse fetchTopicMetadata(Set<String> topics, List<Broker> brokers, String clientId, int timeoutMs,
                          int correlationId) throws JsonProcessingException {
        Properties props = new Properties();
        props.put("metadata.broker.list", JacksonUtils.objToJson(brokers));
        props.put("client.id", clientId);
        props.put("request.timeout.ms", timeoutMs+"");
        ProducerConfig producerConfig = new ProducerConfig(props);
        return fetchTopicMetadata(topics, brokers, producerConfig, correlationId);
    }

    /**
     * Parse a list of broker urls in the form host1:port1, host2:port2, ...
     */
    public static List<Broker> parseBrokerList(String brokerListStr){
        List<Broker> list = new ArrayList<>();
        List<String> brokersStr = Utils.parseCsvList(brokerListStr);
        for(int i = 0;i < brokersStr.size();i++){
            String brokerStr = brokersStr.get(i);
            int brokerId = i;
            String[] brokerInfos = brokerStr.split(":");
            String hostName = brokerInfos[0];
            int port = Integer.parseInt(brokerInfos[1]);
            list.add(new Broker(brokerId, hostName, port));
        }
       return list;
    }

}

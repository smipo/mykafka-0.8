package kafka.javaapi.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.message.Message;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZookeeperConsumerConnector implements ConsumerConnector {

    ConsumerConfig config;
    boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) throws UnknownHostException {
        this.config = config;
        this.enableFetcher = enableFetcher;
        underlying = new kafka.consumer.ZookeeperConsumerConnector(config, enableFetcher);
    }
    public ZookeeperConsumerConnector(ConsumerConfig config)throws UnknownHostException{
        this(config,true);
    }
    kafka.consumer.ZookeeperConsumerConnector underlying;

    public <T> Map<String, List<KafkaStream<T>>> createMessageStreams(
            Map<String, Integer> topicCountMap, Decoder<T> decoder)throws InterruptedException{
        Map<String, Integer> scalaTopicCountMap = new HashMap<>();
        scalaTopicCountMap.putAll(topicCountMap);
        Map<String,List<KafkaStream<T>>> scalaReturn = underlying.consume(scalaTopicCountMap, decoder);
        Map<String,List<KafkaStream<T>>> ret = new HashMap<>();
        for (Map.Entry<String,List<KafkaStream<T>>> entry : scalaReturn.entrySet()) {
            String topic = entry.getKey();
            List<KafkaStream<T>> streams = entry.getValue();
            List<KafkaStream<T>> javaStreamList = new java.util.ArrayList<>();
            for (KafkaStream<T> stream : streams)
                javaStreamList.add(stream);
            ret.put(topic, javaStreamList);
        }
        return ret;
    }
    public Map<String, List<KafkaStream<Message>>> createMessageStreams(
            Map<String, Integer> topicCountMap)throws InterruptedException{
        return  createMessageStreams(topicCountMap, new DefaultDecoder());
    }

    public <T> List<KafkaStream<T>> createMessageStreamsByFilter(
            TopicFilter topicFilter, int numStreams, Decoder<T> decoder)  throws Exception{
        return underlying.createMessageStreamsByFilter(topicFilter, numStreams, decoder);
    }
    public List<KafkaStream<Message>> createMessageStreamsByFilter(
            TopicFilter topicFilter, int numStreams) throws Exception{
        return createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder());
    }
    public List<KafkaStream<Message>> createMessageStreamsByFilter(
            TopicFilter topicFilter) throws Exception{
       return createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder());
    }

    /**
     *  Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets(){
        underlying.commitOffsets();
    }

    /**
     *  Shut down the connector
     */
    public void shutdown(){
        underlying.shutdown();
    }
}

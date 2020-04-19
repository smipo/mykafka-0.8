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
/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 */
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


    @Override
    public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder) throws InterruptedException {
        Map<String,List<KafkaStream<K,V>>> scalaReturn = underlying.consume(topicCountMap, keyDecoder, valueDecoder);
        Map<String, List<KafkaStream<K, V>>> ret = new HashMap<>();
        for(Map.Entry<String,List<KafkaStream<K,V>>> entry : scalaReturn.entrySet()){
            List<KafkaStream<K,V>> javaStreamList = new java.util.ArrayList<>();
            for (KafkaStream<K,V> stream : entry.getValue())
                javaStreamList.add(stream);
            ret.put(entry.getKey(), javaStreamList);
        }
        return ret;
    }

    @Override
    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) throws InterruptedException {
        return createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
    }

    @Override
    public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder) throws Exception {
        return  underlying.createMessageStreamsByFilter(topicFilter, numStreams, keyDecoder, valueDecoder);
    }

    @Override
    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams) throws Exception {
        return createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder(), new DefaultDecoder());
    }

    @Override
    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) throws Exception {
        return   createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder());
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

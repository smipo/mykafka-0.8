package kafka.producer.async;

import kafka.api.ProducerRequest;
import kafka.api.TopicMetadata;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.NoCompressionCodec;
import kafka.producer.*;
import kafka.serializer.Encoder;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultEventHandler<K,V> implements  EventHandler<K,V>{

    private static Logger logger = Logger.getLogger(DefaultEventHandler.class);

    public ProducerConfig config;
    public Partitioner<K> partitioner;
    public Encoder<V> encoder;
    public Encoder<K> keyEncoder;
    public ProducerPool producerPool;
    public Map<String, TopicMetadata> topicPartitionInfos = new HashMap<>();

    public DefaultEventHandler(ProducerConfig config, Partitioner<K> partitioner, Encoder<V> encoder, Encoder<K> keyEncoder, ProducerPool producerPool, Map<String, TopicMetadata> topicPartitionInfos) {
        this.config = config;
        this.partitioner = partitioner;
        this.encoder = encoder;
        this.keyEncoder = keyEncoder;
        this.producerPool = producerPool;
        this.topicPartitionInfos = topicPartitionInfos;

        isSync = ("sync" == config.producerType);

        brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos);
        topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs;
    }
    public boolean isSync;

    AtomicInteger correlationId = new AtomicInteger(0);
    public BrokerPartitionInfo brokerPartitionInfo ;

    public int topicMetadataRefreshInterval ;
    public long lastTopicMetadataRefreshTime = 0L;
    public Set<String> topicMetadataToRefresh = new HashSet<>();
    public Map<String,Integer> sendPartitionPerTopicCache = new HashMap<>();
}

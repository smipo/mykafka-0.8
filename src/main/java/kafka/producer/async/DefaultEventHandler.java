package kafka.producer.async;

import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import kafka.api.ProducerRequest;
import kafka.api.ProducerResponse;
import kafka.api.TopicMetadata;
import kafka.common.*;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.NoCompressionCodec;
import kafka.producer.*;
import kafka.serializer.Encoder;
import kafka.utils.Pair;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private Random random = new Random();


    public void handle(List<KeyedMessage<K,V>> events)  throws Exception{
        List<KeyedMessage<K, Message>> serializedData = serialize(events);
        List<KeyedMessage<K, Message>> outstandingProduceRequests = serializedData;
        int remainingRetries = config.messageSendMaxRetries + 1;
        int correlationIdStart = correlationId.get();
        logger.debug("Handling %d events".format(events.size()+""));
        while (remainingRetries > 0 && outstandingProduceRequests.size() > 0) {
            outstandingProduceRequests.forEach(t->topicMetadataToRefresh.add(t.topic));
            if (topicMetadataRefreshInterval >= 0 &&
                    System.currentTimeMillis() - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
                brokerPartitionInfo.updateInfo(topicMetadataToRefresh.stream().collect(Collectors.toSet()), correlationId.getAndIncrement());
                sendPartitionPerTopicCache.clear();
                topicMetadataToRefresh.clear();
                lastTopicMetadataRefreshTime = System.currentTimeMillis();
            }
            outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests);
            if (outstandingProduceRequests.size() > 0) {
                logger.info("Back off for %d ms before retrying send. Remaining retries = %d".format(config.retryBackoffMs+"", remainingRetries-1));
                // back off and update the topic metadata cache before attempting another send operation
                Thread.sleep(config.retryBackoffMs);
                // get topics of the outstanding produce requests and refresh metadata for those
                brokerPartitionInfo.updateInfo(outstandingProduceRequests.stream().map(t->t.topic).collect(Collectors.toSet()), correlationId.getAndIncrement());
                sendPartitionPerTopicCache.clear();
                remainingRetries -= 1;
            }
        }
        if(outstandingProduceRequests.size() > 0) {
            int correlationIdEnd = correlationId.get();
            logger.error("Failed to send requests for topics %s with correlation ids in [%d,%d]"
                    .format(outstandingProduceRequests.stream().map(t->t.topic).collect(Collectors.toSet()).toString(),
                            correlationIdStart, correlationIdEnd-1));
            throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.");
        }
    }

    private List<KeyedMessage<K, Message>>  dispatchSerializedData(List<KeyedMessage<K, Message>> messages) {
        Map<Integer,Map<TopicAndPartition,List<KeyedMessage<K, Message>>>> partitionedData = partitionAndCollate(messages);
        if(partitionedData != null){
            List<KeyedMessage<K, Message>> failedProduceRequests = new ArrayList<>();
            try {
                for(Map.Entry<Integer,Map<TopicAndPartition,List<KeyedMessage<K, Message>>>> entry : partitionedData.entrySet()){
                    Map<TopicAndPartition, ByteBufferMessageSet> messageSetPerBroker = groupMessagesToSet(entry.getValue());

                    List<TopicAndPartition> failedTopicPartitions = send(entry.getKey(), messageSetPerBroker);
                    for(TopicAndPartition topicPartition:failedTopicPartitions){
                        List<KeyedMessage<K, Message>> data = entry.getValue().get(topicPartition);
                        if(data != null){
                            failedProduceRequests.addAll(data);
                        }
                    }
                }
            } catch(Throwable t) {
                logger.error("Failed to send messages", t);
            }
            return failedProduceRequests;
        }else{
            return messages;
        }
    }

    public List<KeyedMessage<K, Message>> serialize(List<KeyedMessage<K,V>> events) {
        List<KeyedMessage<K, Message>> serializedMessages = new ArrayList<>();
        for(KeyedMessage<K,V> e:events){
            try {
                if(e.hasKey())
                    serializedMessages.add(new KeyedMessage<>(e.topic, e.key, new Message(keyEncoder.toBytes(e.key), encoder.toBytes(e.message))));
                else
                    serializedMessages.add(new KeyedMessage<>( e.topic, null,  new Message( encoder.toBytes(e.message))));
            } catch (Throwable t){
                if (isSync) {
                    throw t;
                } else {
                    // currently, if in async mode, we just log the serialization error. We need to revisit
                    // this when doing kafka-496
                    logger.error("Error serializing message for topic %s".format(e.topic), t);
                }
            }
        }
        return serializedMessages;
    }

    public Map<Integer,Map<TopicAndPartition,List<KeyedMessage<K, Message>>>> partitionAndCollate(List<KeyedMessage<K, Message>> messages) {
        Map<Integer,Map<TopicAndPartition,List<KeyedMessage<K, Message>>>> ret = new HashMap<>();
        try {
            for (KeyedMessage<K, Message> message : messages) {
                List<BrokerPartitionInfo.PartitionAndLeader> topicPartitionsList = getPartitionListForTopic(message);
                int partitionIndex = getPartition(message.topic, message.key, topicPartitionsList);
                BrokerPartitionInfo.PartitionAndLeader brokerPartition = topicPartitionsList.get(partitionIndex);

                // postpone the failure until the send operation, so that requests for other brokers are handled correctly
                int leaderBrokerId = -1;
                if(brokerPartition.leaderBrokerIdOpt != null){
                    leaderBrokerId = brokerPartition.leaderBrokerIdOpt;
                }

                Map<TopicAndPartition,List<KeyedMessage<K, Message>>> dataPerBroker = new HashMap<>();
                Map<TopicAndPartition,List<KeyedMessage<K, Message>>> element = ret.get(leaderBrokerId);
                if(element != null && !element.isEmpty()){
                    dataPerBroker.putAll(element);
                }else{
                    ret.put(leaderBrokerId, dataPerBroker);
                }
                TopicAndPartition topicAndPartition = new TopicAndPartition(message.topic, brokerPartition.partitionId);
                List<KeyedMessage<K, Message>> dataPerTopicPartition = new ArrayList<>();
                List<KeyedMessage<K, Message>> elementList = dataPerBroker.get(topicAndPartition) ;
                if(elementList != null && !elementList.isEmpty()){
                    dataPerTopicPartition.addAll(elementList);
                }else{
                    dataPerBroker.put(topicAndPartition, dataPerTopicPartition);
                }
                dataPerTopicPartition.add(message);
            }
            return ret;
        }catch (UnknownTopicOrPartitionException ute){    // Swallow recoverable exceptions and return None so that they can be retried.
            logger.warn("Failed to collate messages by topic,partition due to: " + ute.getMessage());
        }catch (LeaderNotAvailableException lnae){
            logger.warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage());
        }catch (Throwable oe){
            logger.error("Failed to collate messages by topic, partition due to: " + oe.getMessage());
        }
        return null;
    }

    private List<BrokerPartitionInfo.PartitionAndLeader> getPartitionListForTopic(KeyedMessage<K, Message> m) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<BrokerPartitionInfo.PartitionAndLeader> topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement());
        logger.debug("Broker partitions registered for topic: %s are %s"
                .format(m.topic, topicPartitionsList.toString()));
        int totalNumPartitions = topicPartitionsList.size();
        if(totalNumPartitions == 0)
            throw new NoBrokersForPartitionException("Partition key = " + m.key);
        return topicPartitionsList;
    }

    /**
     * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
     * the value of partition is not between 0 and numPartitions-1
     * @param key the partition key
     * @param topicPartitionList the list of available partitions
     * @return the partition id
     */
    private int getPartition(String topic,K key, List<BrokerPartitionInfo.PartitionAndLeader>topicPartitionList) {
        int numPartitions = topicPartitionList.size();
        if(numPartitions <= 0)
            throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist");
        int partition ;
        if(key == null) {
            // If the key is null, we don't really need a partitioner
            // So we look up in the send partition cache for the topic to decide the target partition
            Integer id = sendPartitionPerTopicCache.get(topic);
            if(id != null){
                // directly return the partitionId without checking availability of the leader,
                // since we want to postpone the failure until the send operation anyways
                partition = id;
            }else{
                List<BrokerPartitionInfo.PartitionAndLeader> availablePartitions = topicPartitionList.stream().filter(p->p.leaderBrokerIdOpt!=null).collect(Collectors.toList());
                if (availablePartitions.isEmpty())
                    throw new LeaderNotAvailableException("No leader for any partition in topic " + topic);
                int index = Utils.abs(random.nextInt()) % availablePartitions.size();
                int partitionId = availablePartitions.get(index).partitionId;
                sendPartitionPerTopicCache.put(topic, partitionId);
                partition =  partitionId;
            }
        } else
            partition = partitioner.partition(key, numPartitions);
        if(partition < 0 || partition >= numPartitions)
            throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
                    "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]");
        logger.trace("Assigning message of topic %s and key %s to a selected partition %d".format(topic, key == null? "[none]" : key.toString(), partition));
        return partition;
    }

    /**
     * Constructs and sends the produce request based on a map from (topic, partition) -> messages
     *
     * @param brokerId the broker that will receive the request
     * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
     * @return the set (topic, partitions) messages which incurred an error sending or processing
     */
    private List<TopicAndPartition> send(int brokerId, Map<TopicAndPartition, ByteBufferMessageSet> messagesPerTopic)  {
        if(brokerId < 0) {
            logger.warn("Failed to send data since partitions %s don't have a leader".format(messagesPerTopic.toString()));
            return messagesPerTopic.keySet().stream().collect(Collectors.toList());
        } else if(messagesPerTopic.size() > 0) {
            int currentCorrelationId = correlationId.getAndIncrement();
            ProducerRequest producerRequest = new ProducerRequest(currentCorrelationId, config.clientId, config.requestRequiredAcks,
                    config.requestTimeoutMs, messagesPerTopic);
            List<TopicAndPartition>  failedTopicPartitions = new ArrayList<>();
            try {
                SyncProducer syncProducer = producerPool.getProducer(brokerId);
                logger.debug("Producer sending messages with correlation id %d for topics %s to broker %d on %s:%d"
                        .format(currentCorrelationId+"", messagesPerTopic.keySet().toString(), brokerId, syncProducer.config.host, syncProducer.config.port));
                ProducerResponse response = syncProducer.send(producerRequest);
                logger.debug("Producer sent messages with correlation id %d for topics %s to broker %d on %s:%d"
                        .format(currentCorrelationId+"", messagesPerTopic.keySet().toString(), brokerId, syncProducer.config.host, syncProducer.config.port));
                if(response != null) {
                    if (response.status.size() != producerRequest.data.size())
                        throw new KafkaException("Incomplete response (%s) for producer request (%s)".format(response.toString(), producerRequest.toString()));
                    for(Map.Entry<TopicAndPartition, ProducerResponse.ProducerResponseStatus> entry : response.status.entrySet()){
                        if(entry.getValue().error != ErrorMapping.NoError){
                            failedTopicPartitions.remove(entry.getKey());
                        }
                    }
                    if(failedTopicPartitions.size() > 0) {
                        logger.warn("Produce request with correlation id %d failed due to %s".format(currentCorrelationId+"", failedTopicPartitions.toString()));
                    }
                    return failedTopicPartitions;
                } else
                    return new ArrayList<>();
            } catch (Throwable t){
                logger.warn("Failed to send producer request with correlation id %d to broker %d with data for partitions %s"
                        .format(currentCorrelationId+"", brokerId, messagesPerTopic.toString(), t));
                return messagesPerTopic.keySet().stream().collect(Collectors.toList());
            }
        } else {
            return new ArrayList<>();
        }
    }

    private Map<TopicAndPartition, ByteBufferMessageSet> groupMessagesToSet(Map<TopicAndPartition, List<KeyedMessage<K,Message>>> messagesPerTopicAndPartition) throws IOException {
        /** enforce the compressed.topics config here.
         *  If the compression codec is anything other than NoCompressionCodec,
         *    Enable compression only for specified topics if any
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */
        Map<TopicAndPartition, ByteBufferMessageSet> messagesPerTopicPartition = new HashMap<>();
        for(Map.Entry<TopicAndPartition, List<KeyedMessage<K,Message>>> entry : messagesPerTopicAndPartition.entrySet()){
            TopicAndPartition topicAndPartition = entry.getKey();
            List<KeyedMessage<K,Message>> messages= entry.getValue();
            List<Message> rawMessages = messages.stream().map(k->k.message).collect(Collectors.toList());
            if(config.compressionCodec instanceof NoCompressionCodec){
                logger.debug("Sending %d messages with no compression to %s".format(messages.size()+"", topicAndPartition));
                Message[] ms = new Message[rawMessages.size()];
                messagesPerTopicPartition.put(topicAndPartition,new ByteBufferMessageSet(new NoCompressionCodec(), rawMessages.toArray(ms)));
            }else{
                if(config.compressedTopics.size() == 0){
                    logger.debug("Sending %d messages with compression codec %d to %s"
                            .format(messages.size()+"", config.compressionCodec.codec(), topicAndPartition));
                    Message[] ms = new Message[rawMessages.size()];
                    messagesPerTopicPartition.put(topicAndPartition,new ByteBufferMessageSet(config.compressionCodec, rawMessages.toArray(ms)));
                }else{
                    if(config.compressedTopics.contains(topicAndPartition.topic())) {
                        logger.debug("Sending %d messages with compression codec %d to %s"
                                .format(messages.size()+"", config.compressionCodec.codec(), topicAndPartition));
                        Message[] ms = new Message[rawMessages.size()];
                        messagesPerTopicPartition.put(topicAndPartition,new ByteBufferMessageSet(config.compressionCodec, rawMessages.toArray(ms)));
                    }
                    else {
                        logger.debug("Sending %d messages to %s with no compression as it is not in compressed.topics - %s"
                                .format(messages.size()+"", topicAndPartition, config.compressedTopics.toString()));
                        Message[] ms = new Message[rawMessages.size()];
                        messagesPerTopicPartition.put(topicAndPartition,new ByteBufferMessageSet(new NoCompressionCodec(), rawMessages.toArray(ms)));
                    }
                }
            }
        }
        return messagesPerTopicPartition;
    }

    public void close() {
        if (producerPool != null)
            producerPool.close();
    }
}

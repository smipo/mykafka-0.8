package kafka.producer;

import kafka.api.ProducerRequest;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.common.InvalidPartitionException;
import kafka.common.NoBrokersForPartitionException;
import kafka.producer.async.CallbackHandler;
import kafka.producer.async.EventHandler;
import kafka.serializer.Encoder;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producer<K,V> {

    private static Logger logger = Logger.getLogger(Producer.class);

    ProducerConfig config;
    Partitioner<K> partitioner;
    ProducerPool<V> producerPool;
    boolean populateProducerPool;
    private BrokerPartitionInfo brokerPartitionInfo;

    public Producer( ProducerConfig config,
                     Partitioner<K> partitioner,
                     ProducerPool<V> producerPool,
                     boolean populateProducerPool,
                     BrokerPartitionInfo brokerPartitionInfo){
        init(config,
                partitioner,
                 producerPool,
         populateProducerPool,
         brokerPartitionInfo);
    }
    /**
     * This constructor can be used when all config parameters will be specified through the
     * ProducerConfig object
     * @param config Producer Configuration object
     */
    public Producer(ProducerConfig config)throws ClassNotFoundException,InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        init(config, Utils.getObject(config.partitionerClass) == null?null:(Partitioner<K>)Utils.getObject(config.partitionerClass),
                new ProducerPool<V>(config, Utils.getObject(config.serializerClass) == null?null:(Encoder<V>)Utils.getObject(config.serializerClass)), true, null);
    }

    /**
     * This constructor can be used to provide pre-instantiated objects for all config parameters
     * that would otherwise be instantiated via reflection. i.e. encoder, partitioner, event handler and
     * callback handler. If you use this constructor, encoder, eventHandler, callback handler and partitioner
     * will not be picked up from the config.
     * @param config Producer Configuration object
     * @param encoder Encoder used to convert an object of type V to a kafka.message.Message. If this is null it
     * throws an InvalidConfigException
     * @param eventHandler the class that implements kafka.producer.async.IEventHandler[T] used to
     * dispatch a batch of produce requests, using an instance of kafka.producer.SyncProducer. If this is null, it
     * uses the DefaultEventHandler
     * @param cbkHandler the class that implements kafka.producer.async.CallbackHandler[T] used to inject
     * callbacks at various stages of the kafka.producer.AsyncProducer pipeline. If this is null, the producer does
     * not use the callback handler and hence does not invoke any callbacks
     * @param partitioner class that implements the kafka.producer.Partitioner[K], used to supply a custom
     * partitioning strategy on the message key (of type K) that is specified through the ProducerData[K, T]
     * object in the  send API. If this is null, producer uses DefaultPartitioner
     */
    public Producer(ProducerConfig config,
                    Encoder<V> encoder,
                    EventHandler<V> eventHandler,
                    CallbackHandler<V> cbkHandler,
                    Partitioner<K> partitioner) {
        if(partitioner == null) partitioner = new DefaultPartitioner<K>();
        init(config,partitioner ,
                new ProducerPool<V>(config, encoder, eventHandler, cbkHandler), true, null);
    }

    private AtomicBoolean hasShutdown = new AtomicBoolean(false);
    private Random random = new Random();
    // check if zookeeper based auto partition discovery is enabled
    private boolean zkEnabled ;

    private void init(ProducerConfig config,
                      Partitioner<K> partitioner,
                      ProducerPool<V> producerPool,
                      boolean populateProducerPool,
                      BrokerPartitionInfo brokerPartitionInfo){
        this.config = config;
        this.partitioner = partitioner;
        this.producerPool = producerPool;
        this.populateProducerPool = populateProducerPool;
        this.brokerPartitionInfo = brokerPartitionInfo;

        this.zkEnabled = Utils.propertyExists(config.zkConnect);
        if(this.brokerPartitionInfo == null) {
            if(this.zkEnabled){
                Properties zkProps = new Properties();
                zkProps.put("zk.connect", config.zkConnect);
                zkProps.put("zk.sessiontimeout.ms", config.zkSessionTimeoutMs);
                zkProps.put("zk.connectiontimeout.ms", config.zkConnectionTimeoutMs);
                zkProps.put("zk.synctime.ms", config.zkSyncTimeMs);
                this.brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZkUtils.ZKConfig(zkProps),new ArrayList<>());
            }else{
                this.brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
            }
        }

        // pool of producers, one per broker
        if(populateProducerPool) {
            Map<Integer, Broker> allBrokers = this.brokerPartitionInfo.getAllBrokerInfo();
            allBrokers.values().forEach(b -> producerPool.addProducer(new Broker(b.id(), b.host(), b.host(), b.port())));
        }
    }

    /**
     * Sends the data, partitioned by key to the topic using either the
     * synchronous or the asynchronous producer
     * @param producerData the producer data object that encapsulates the topic, key and message data
     */
    public void send(ProducerData<K,V> ...producerData) throws Exception{
        if(zkEnabled){
            zkSend(producerData);
        }else{
            configSend(producerData);
        }

    }

    private void zkSend(ProducerData<K,V> ...producerData) throws Exception{
        ProducerPool.ProducerPoolData[] producerPoolRequests = new ProducerPool.ProducerPoolData[producerData.length];
        for(int i = 0;i < producerData.length;i++){
            ProducerData<K,V> pd = producerData[i];
            Partition brokerIdPartition = null;
            Broker brokerInfoOpt = null;

            int numRetries = 0;
            while(numRetries <= config.zkReadRetries && brokerInfoOpt == null) {
                if(numRetries > 0) {
                    logger.info("Try #" + numRetries + " ZK producer cache is stale. Refreshing it by reading from ZK again");
                    brokerPartitionInfo.updateInfo();
                }

                Partition[] topicPartitionsList = getPartitionListForTopic(pd);
                int totalNumPartitions = topicPartitionsList.length;

                int partitionId = getPartition(pd.getKey(), totalNumPartitions);
                brokerIdPartition = topicPartitionsList[partitionId];
                brokerInfoOpt = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId());
                numRetries += 1;
            }
            if(brokerInfoOpt != null){
                logger.debug("Sending message to broker " + brokerInfoOpt.host() + ":" + brokerInfoOpt.port() +
                        " on partition " + brokerIdPartition.partId());
            }else{
                throw new NoBrokersForPartitionException("Invalid Zookeeper state. Failed to get partition for topic: " +
                        pd.getTopic() + " and key: " + pd.getKey());
            }
            producerPoolRequests[i] = producerPool.getProducerPoolData(pd.getTopic(),
                    new Partition(brokerIdPartition.brokerId(), brokerIdPartition.partId()),
                    pd.getData());
        }
        producerPool.send(producerPoolRequests);
    }

    private void configSend(ProducerData<K,V> ...producerData) throws Exception{
        ProducerPool.ProducerPoolData[] producerPoolRequests = new ProducerPool.ProducerPoolData[producerData.length];
        for(int i = 0;i < producerData.length;i++){
            ProducerData<K,V> pd = producerData[i];
            // find the broker partitions registered for this topic
            Partition[] topicPartitionsList = getPartitionListForTopic(pd);
            int totalNumPartitions = topicPartitionsList.length;

            int randomBrokerId = random.nextInt(totalNumPartitions);
            Partition brokerIdPartition = topicPartitionsList[randomBrokerId];
            Broker brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId());

            logger.debug("Sending message to broker " + brokerInfo.host() + ":" + brokerInfo.port() +
                    " on a randomly chosen partition");
            int partition = ProducerRequest.RandomPartition;
            logger.debug("Sending message to broker " + brokerInfo.host() + ":" + brokerInfo.port() + " on a partition " +
                    brokerIdPartition.partId());
            producerPoolRequests[i] = producerPool.getProducerPoolData(pd.getTopic(),
                    new Partition(brokerIdPartition.brokerId(), partition),
                    pd.getData());
        }
        producerPool.send(producerPoolRequests);
    }

    private Partition[] getPartitionListForTopic(ProducerData<K,V> pd){
        logger.debug("Getting the number of broker partitions registered for topic: " + pd.getTopic());
        SortedSet<Partition> topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic());
        logger.debug("Broker partitions registered for topic: " + pd.getTopic() + " = " + topicPartitionsList.toString());
        int totalNumPartitions = topicPartitionsList.size();
        if(totalNumPartitions == 0) throw new NoBrokersForPartitionException("Partition = " + pd.getKey());
        Partition[] partitions = new Partition[topicPartitionsList.size()];
        return topicPartitionsList.toArray(partitions);
    }

    /**
     * Retrieves the partition id and throws an InvalidPartitionException if
     * the value of partition is not between 0 and numPartitions-1
     * @param key the partition key
     * @param numPartitions the total number of available partitions
     * @returns the partition id
     */
    private int getPartition(K key, int numPartitions) {
        if(numPartitions <= 0)
            throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions +
                    "\n Valid values are > 0");
        int partition ;
        if(key == null) partition = random.nextInt(numPartitions);
        else partition = partitioner.partition(key , numPartitions);
        if(partition < 0 || partition >= numPartitions)
            throw new InvalidPartitionException("Invalid partition id : " + partition +
                    "\n Valid values are in the range inclusive [0, " + (numPartitions-1) + "]");
       return partition;
    }

    /**
     * Callback to add a new producer to the producer pool. Used by ZKBrokerPartitionInfo
     * on registration of new broker in zookeeper
     * @param bid the id of the broker
     * @param host the hostname of the broker
     * @param port the port of the broker
     */
    private void producerCbk(int bid, String host, int port)   {
        if(populateProducerPool) producerPool.addProducer(new Broker(bid, host, host, port));
        else logger.debug("Skipping the callback since populateProducerPool = false");
    }

    /**
     * Close API to close the producer pool connections to all Kafka brokers. Also closes
     * the zookeeper client connection if one exists
     */
    public void close()  throws InterruptedException{
        boolean canShutdown = hasShutdown.compareAndSet(false, true);
        if(canShutdown) {
            producerPool.close();
            brokerPartitionInfo.close();
        }
    }

}

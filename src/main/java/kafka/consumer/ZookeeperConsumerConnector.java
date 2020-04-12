package kafka.consumer;

import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.cluster.Partition;
import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.InvalidConfigException;
import kafka.common.TopicAndPartition;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;



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
public class ZookeeperConsumerConnector implements ConsumerConnector{

    private static Logger logger = Logger.getLogger(ZookeeperConsumerConnector.class);

    public  static FetchedDataChunk shutdownCommand = new FetchedDataChunk(null, null, -1L);

    ConsumerConfig config;
    boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) throws UnknownHostException {
        this.config = config;
        this.enableFetcher = enableFetcher;

        String consumerUuid  = null;
        if(config.consumerId != null && !config.consumerId.isEmpty()){
            consumerUuid = config.consumerId;
        }else{
            UUID uuid = UUID.randomUUID();
            consumerUuid = "%s-%d-%s".format(
                    InetAddress.getLocalHost().getHostName(), System.currentTimeMillis(),
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0,8));
        }

        this.consumerIdString = config.groupId + "_" + consumerUuid;
        connectZk();
        createFetcher();
        if (config.autoCommitEnable) {
            logger.info("starting auto committer every " + config.autoCommitIntervalMs + " ms");
            scheduler.scheduleWithRate(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(scheduler.currentThreadName("Kafka-consumer-autocommit-"));
                    autoCommit();
                }
            }, config.autoCommitIntervalMs, config.autoCommitIntervalMs,true);
        }
    }
    public ZookeeperConsumerConnector(ConsumerConfig config)  throws UnknownHostException {
        this(config, true);
    }

    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private ConsumerFetcherManager fetcher = null;
    private ZkClient zkClient = null;
    private Pool<String, Pool<Integer, PartitionTopicInfo>> topicRegistry = new Pool<>();
    Pool<TopicAndPartition, Long> checkpointedOffsets = new Pool<TopicAndPartition, Long>();
    // topicThreadIdAndQueues : (topic,consumerThreadId) -> queue
    private Pool<Pair<String,String>, BlockingQueue<FetchedDataChunk>> topicThreadIdAndQueues = new Pool<Pair<String,String>, BlockingQueue<FetchedDataChunk>>();
    private KafkaScheduler scheduler = new KafkaScheduler(1);
    private AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

    private ZKSessionExpireListener sessionExpirationListener = null;
    private ZKRebalancerListener loadBalancerListener = null;
    private ZKTopicPartitionChangeListener topicPartitionChangeListenner = null;
    private ZookeeperTopicEventWatcher wildcardTopicWatcher = null;

    private String consumerIdString;

    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) throws InterruptedException {
        return createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
    }

    public <K,V> Map<String, List<KafkaStream<K,V>>> createMessageStreams(
            Map<String, Integer> topicCountMap,  Decoder<K> keyDecoder,
            Decoder<V>  valueDecoder)throws InterruptedException{
        if (messageStreamCreated.getAndSet(true))
            throw new RuntimeException(this.getClass().getSimpleName() +
                    " can create message streams at most once");
        return consume(topicCountMap, keyDecoder,valueDecoder);
    }

    public <K,V> List<KafkaStream<K,V>> createMessageStreamsByFilter(
            TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder,
            Decoder<V>  valueDecoder) throws Exception{
        WildcardStreamsHandler<K,V> wildcardStreamsHandler = new WildcardStreamsHandler<>(topicFilter, numStreams, keyDecoder,valueDecoder);
        return wildcardStreamsHandler.streams();
    }

    private void createFetcher() {
        if (enableFetcher)
            fetcher = new ConsumerFetcherManager(consumerIdString, config, zkClient);
    }

    private void connectZk() {
        logger.info("Connecting to zookeeper instance at " + config.zkConnect);
        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());
    }

    public void shutdown() {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (canShutdown) {
            logger.info("ZKConsumerConnector shutting down");

            if (wildcardTopicWatcher != null)
                wildcardTopicWatcher.shutdown();
            try {
                scheduler.shutdownNow();
                if(fetcher != null){
                    fetcher.stopConnections();
                }
                sendShutdownToAllQueues();
                if (config.autoCommitEnable)
                    commitOffsets();
                if (zkClient != null) {
                    zkClient.close();
                    zkClient = null;
                }
            }
            catch (Exception e){
                logger.fatal("error during consumer connector shutdown", e);
            }
            logger.info("ZKConsumerConnector shut down completed");
        }
    }

    public <K,V> Map<String,List<KafkaStream<K,V>>> consume(Map<String,Integer> topicCountMap,
                                                            Decoder<K> keyDecoder,  Decoder<V> valueDecoder) throws InterruptedException{
        logger.debug("entering consume ");
        if (topicCountMap == null)
            throw new RuntimeException("topicCountMap is null");

        StaticTopicCount topicCount = TopicCountFactory.constructTopicCount(consumerIdString, topicCountMap);

        Map<String, Set<String>> topicThreadIds = topicCount.getConsumerThreadIdsPerTopic();

        // make a list of (queue,stream) pairs, one pair for each threadId
        List<Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>> queuesAndStreams = new ArrayList<>();
        for (Map.Entry<String,Set<String>> entry : topicThreadIds.entrySet()) {
            Set<String> threadIdSet = entry.getValue();
            for(String threadId:threadIdSet){
                BlockingQueue<FetchedDataChunk> queue =  new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                KafkaStream<K,V> stream = new KafkaStream<K,V>(queue, config.consumerTimeoutMs, keyDecoder,valueDecoder, config.clientId);
                queuesAndStreams.add(new Pair<>(queue,stream));
            }
        }
        ZkUtils.ZKGroupDirs dirs = new ZkUtils.ZKGroupDirs(config.groupId);
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        reinitializeConsumer(topicCount, queuesAndStreams);

        return loadBalancerListener.kafkaMessageAndMetadataStreams;
    }

    private <K,V> void reinitializeConsumer (TopicCount topicCount,
                                             List<Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>>  queuesAndStreams) throws InterruptedException{

        ZkUtils.ZKGroupDirs dirs = new ZkUtils.ZKGroupDirs(config.groupId);

        // listener to consumer and partition changes
        if (loadBalancerListener == null) {
            Map<String,List<KafkaStream<K,V>>> topicStreamsMap = new HashMap<>();
            loadBalancerListener = new ZKRebalancerListener(config.groupId, consumerIdString, topicStreamsMap);
        }
        // create listener for topic partition change event if not exist yet
        if (topicPartitionChangeListenner == null)
            topicPartitionChangeListenner = new ZKTopicPartitionChangeListener(loadBalancerListener);
        // register listener for session expired event
        if (sessionExpirationListener == null)
            sessionExpirationListener = new ZKSessionExpireListener(
                    dirs, consumerIdString, topicCount, loadBalancerListener);

        Map<String, List<KafkaStream<K,V>>> topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams;

        // map of {topic -> Set(thread-1, thread-2, ...)}
        Map<String, Set<String>> consumerThreadIdsPerTopic  = topicCount.getConsumerThreadIdsPerTopic();

        // iterator over (topic, thread-id) tuples
        List<Pair<String,String>> topicThreadIds = new ArrayList<>();
        for(Map.Entry<String, Set<String>> entry : consumerThreadIdsPerTopic.entrySet()){
            String topic = entry.getKey();
            for(String threadId:entry.getValue()){
                topicThreadIds.add(new Pair<>(topic,threadId));
            }
        }
        // list of (pairs of pairs): e.g., ((topic, thread-id),(queue, stream))
        List<Pair<Pair<String,String>,Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>>> threadQueueStreamPairs = new ArrayList<>();
        if(topicCount instanceof WildcardTopicCount){
            for(Pair<String,String> topicThreadId : topicThreadIds){
                for(Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>> qs:queuesAndStreams){
                    threadQueueStreamPairs.add(new Pair<>(topicThreadId,qs));
                }
            }
        }else{
            int qsSize = queuesAndStreams.size();
            for(int i = 0;i < topicThreadIds.size();i++){
                if(i > qsSize - 1) break;
                threadQueueStreamPairs.add(new Pair<>(topicThreadIds.get(i),queuesAndStreams.get(i)));
            }
        }
        for(Pair<Pair<String,String>,Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>> threadQueueStream:threadQueueStreamPairs){
            topicThreadIdAndQueues.put(threadQueueStream.getKey(), threadQueueStream.getValue().getKey());
        }
        for(Pair<Pair<String,String>,Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>> threadQueueStreamPair:threadQueueStreamPairs){
            String topic = threadQueueStreamPair.getKey().getKey();
            KafkaStream<K,V> stream = threadQueueStreamPair.getValue().getValue();
            List<KafkaStream<K,V>> streams = topicStreamsMap.get(topic);
            if(streams == null){
                streams = new ArrayList<>();
                topicStreamsMap.put(topic,streams);
            }
            streams.add(stream);
        }

        // listener to consumer and partition changes
        zkClient.subscribeStateChanges(sessionExpirationListener);

        zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener);

        topicStreamsMap.entrySet().forEach(entry -> {
            // register on broker partition path changes
            String topic = entry.getKey();
            String partitionPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            zkClient.subscribeDataChanges(partitionPath, topicPartitionChangeListenner);
        });
        // explicitly trigger load balancing for this consumer
        loadBalancerListener.syncedRebalance();
    }
    private void registerConsumerInZK(ZkUtils.ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        logger.info("begin registering consumer " + consumerIdString + " in ZK");
        ZkUtils.createEphemeralPathExpectConflict(zkClient, dirs.consumerRegistryDir + "/" + consumerIdString, topicCount.dbString());
        logger.info("end registering consumer " + consumerIdString + " in ZK");
    }

    private void sendShutdownToAllQueues()  throws InterruptedException {
        for (BlockingQueue<FetchedDataChunk> queue : topicThreadIdAndQueues.values()) {
            logger.debug("Clearing up queue");
            queue.clear();
            queue.put(ZookeeperConsumerConnector.shutdownCommand);
            logger.debug("Cleared queue and sent shutdown command");
        }
    }

    public void autoCommit() {
//        logger.info("auto committing");
        try {
            commitOffsets();
        }
        catch(Throwable t) {
            logger.error("exception during autoCommit: ", t);

        }
    }

    public void commitOffsets() {
        if (zkClient == null) {
            logger.error("zk client is null. Cannot commit offsets");
            return;
        }
        for (Map.Entry<String, Pool<Integer, PartitionTopicInfo>> entry : topicRegistry.pool().entrySet()) {
            String topic = entry.getKey();
            Pool<Integer, PartitionTopicInfo> infos = entry.getValue();
            ZkUtils.ZKGroupTopicDirs topicDirs = new ZkUtils.ZKGroupTopicDirs(config.groupId, topic);
            for (PartitionTopicInfo info : infos.values()) {
                long newOffset = info.getConsumeOffset();
                if (newOffset != checkpointedOffsets.get(new TopicAndPartition(topic, info.partitionId))) {
                    try {
                        ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + info.partitionId,
                                String.valueOf(newOffset));
                        checkpointedOffsets.put(new TopicAndPartition(topic, info.partitionId), newOffset);
                    }
                    catch (Throwable t){
                        // log it and let it go
                        logger.warn("exception during commitOffsets",  t);
                    }
                    logger.debug("Committed offset " + newOffset + " for topic " + info);
                }
            }
        }
    }


    public class ZKSessionExpireListener  implements IZkStateListener {

        ZkUtils.ZKGroupDirs dirs;
        String consumerIdString;
        TopicCount topicCount;
        ZKRebalancerListener loadBalancerListener;

        public ZKSessionExpireListener(ZkUtils.ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount, ZKRebalancerListener loadBalancerListener) {
            this.dirs = dirs;
            this.consumerIdString = consumerIdString;
            this.topicCount = topicCount;
            this.loadBalancerListener = loadBalancerListener;
        }
        public  void handleStateChanged(Watcher.Event.KeeperState var1) throws Exception{
            // do nothing, since zkclient will do reconnect for us.
        }
        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception
         *             On any error.
         */
        public void handleNewSession() throws Exception{
            /**
             *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
             *  connection for us. We need to release the ownership of the current consumer and re-register this
             *  consumer in the consumer registry and trigger a rebalance.
             */
            logger.info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString);
            loadBalancerListener.resetState();
            registerConsumerInZK(dirs, consumerIdString, topicCount);
            // explicitly trigger load balancing for this consumer
            loadBalancerListener.syncedRebalance();

            // There is no need to resubscribe to child and state changes.
            // The child change watchers will be set inside rebalance when we read the children list.
        }

        public void handleSessionEstablishmentError(Throwable var1) throws Exception{
            // do nothing,
        }
    }

    public class ZKRebalancerListener<K,V> implements IZkChildListener {
        String group;
        String consumerIdString;
        Map<String, List<KafkaStream<K,V>>> kafkaMessageAndMetadataStreams;

        public ZKRebalancerListener(String group, String consumerIdString, Map<String, List<KafkaStream<K,V>>> kafkaMessageAndMetadataStreams) {
            this.group = group;
            this.consumerIdString = consumerIdString;
            this.kafkaMessageAndMetadataStreams = kafkaMessageAndMetadataStreams;

            this.watcherExecutorThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    logger.info("starting watcher executor thread for consumer " + consumerIdString);
                    boolean doRebalance = false;
                    while (!isShuttingDown.get()) {
                        try {
                            lock.lock();
                            try {
                                if (!isWatcherTriggered)
                                    cond.await(1000, TimeUnit.MILLISECONDS); // wake up periodically so that it can check the shutdown flag
                            } finally {
                                doRebalance = isWatcherTriggered;
                                isWatcherTriggered = false;
                                lock.unlock();
                            }
                            if (doRebalance)
                                syncedRebalance();
                        } catch (Throwable t){
                            logger.error("error during syncedRebalance", t);
                        }
                    }
                    logger.info("stopping watcher executor thread for consumer " + consumerIdString);
                }
            }, consumerIdString + "_watcher_executor");
            this.watcherExecutorThread.start();
        }
        private boolean isWatcherTriggered = false;
        private ReentrantLock lock = new ReentrantLock();
        private Condition cond = lock.newCondition();
        private Thread watcherExecutorThread ;

        public  void handleChildChange(String parentPath, List<String> curChilds) throws Exception{
            lock.lock();
            try {
                isWatcherTriggered = true;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private void deletePartitionOwnershipFromZK(String topic, int partition) {
            ZkUtils.ZKGroupTopicDirs topicDirs = new ZkUtils.ZKGroupTopicDirs(group, topic);
            String znode = topicDirs.consumerOwnerDir + "/" + partition;
            ZkUtils.deletePath(zkClient, znode);
            logger.debug("Consumer " + consumerIdString + " releasing " + znode);
        }

        private void releasePartitionOwnership(Pool<String, Pool<Integer, PartitionTopicInfo>> localTopicRegistry) {
            logger.info("Releasing partition ownership");
            ConcurrentHashMap<String, Pool<Integer, PartitionTopicInfo>> topics = localTopicRegistry.pool();
            for (Map.Entry<String, Pool<Integer, PartitionTopicInfo>> entry : topics.entrySet()) {
                String topic = entry.getKey();
                Pool<Integer, PartitionTopicInfo> infos = entry.getValue();
                Set<Integer> set = infos.keys();
                for(Integer p:set){
                    deletePartitionOwnershipFromZK(topic, p);
                }
                localTopicRegistry.remove(topic);
            }
        }

        public void resetState() {
            topicRegistry.clear();
        }

        public void syncedRebalance()  throws InterruptedException{
            synchronized(rebalanceLock) {
                for (int i = 0 ;i < config.rebalanceMaxRetries;i++) {
                    logger.info("begin rebalancing consumer " + consumerIdString + " try #" + i);
                    boolean done = false;
                    Cluster cluster = ZkUtils.getCluster(zkClient);
                    try {
                        done = rebalance(cluster);
                    }
                    catch (Exception e){
                        /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                         * For example, a ZK node can disappear between the time we get all children and the time we try to get
                         * the value of a child. Just let this go since another rebalance will be triggered.
                         **/
                        logger.info("exception during rebalance ", e);
                    }
                    logger.info("end rebalancing consumer " + consumerIdString + " try #" + i);
                    if (done) {
                        return;
                    }else {
                        /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                         * clear the cache */
                        logger.info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                    }
                    // stop all fetchers and clear all the queues to avoid data duplication
                    closeFetchersForQueues(cluster,kafkaMessageAndMetadataStreams, topicThreadIdAndQueues.values().stream().collect(Collectors.toList()));
                    Thread.sleep(config.rebalanceBackoffMs);
                }
            }
            throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries +" retries");
        }

        private boolean rebalance(Cluster cluster) throws IOException{
            Map<String, Set<String>> myTopicThreadIdsMap = TopicCountFactory.constructTopicCount(group, consumerIdString, zkClient).getConsumerThreadIdsPerTopic();
            Map<String, List<String>> consumersPerTopicMap = ZkUtils.getConsumersPerTopic(zkClient, group);
            List<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
            if (brokers.size() == 0) {
                // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.
                // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers
                // are up.
                logger.warn("no brokers found when trying to rebalance.");
                zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, loadBalancerListener);
                return true;
            }
            Map<String, Map<Integer,List<Integer>>> partitionsAssignmentPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, myTopicThreadIdsMap.keySet());
            Map<String,List<Integer>> partitionsPerTopicMap = new HashMap<>();
            for(Map.Entry<String, Map<Integer,List<Integer>>> entry : partitionsAssignmentPerTopicMap.entrySet()){
                List<Integer> list = entry.getValue().keySet().stream().collect(Collectors.toList());
                list.sort(Comparator.comparingInt(Integer::byteValue));
                partitionsPerTopicMap.put(entry.getKey(),list);
            }
            /**
             * fetchers must be stopped to avoid data duplication, since if the current
             * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
             * But if we don't stop the fetchers first, this consumer would continue returning data for released
             * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
             */
            closeFetchers(cluster,kafkaMessageAndMetadataStreams, myTopicThreadIdsMap);

            releasePartitionOwnership(topicRegistry);

            Map<Pair<String,Integer>,String> partitionOwnershipDecision = new HashMap<>();
            Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry = new Pool<>();
            for (Map.Entry<String, Set<String>> entry : myTopicThreadIdsMap.entrySet()) {
                String topic = entry.getKey();
                Set<String> consumerThreadIdSet = entry.getValue();

                currentTopicRegistry.put(topic, new Pool<Integer, PartitionTopicInfo>());

                ZkUtils.ZKGroupTopicDirs topicDirs = new ZkUtils.ZKGroupTopicDirs(group, topic);
                List<String> curConsumers = consumersPerTopicMap.get(topic);
                List<Integer> curPartitions = partitionsPerTopicMap.get(topic);

                int nPartsPerConsumer = curPartitions.size() / curConsumers.size();
                int nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

                logger.info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions +
                        " for topic " + topic + " with consumers: " + curConsumers);

                for (String consumerThreadId : consumerThreadIdSet) {
                    int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                    assert(myConsumerPosition >= 0);
                    int startPart = nPartsPerConsumer*myConsumerPosition + Math.min(myConsumerPosition,nConsumersWithExtraPart);
                    int nParts = nPartsPerConsumer;
                    if (myConsumerPosition + 1 <= nConsumersWithExtraPart) nParts += 1;

                    /**
                     *   Range-partition the sorted partitions to consumers for better locality.
                     *  The first few consumers pick up an extra partition, if any.
                     */
                    if (nParts <= 0)
                        logger.warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic);
                    else {
                        for (int i = startPart ; i < startPart + nParts; i++) {
                            Integer partition = curPartitions.get(i);
                            logger.info(consumerThreadId + " attempting to claim partition " + partition);
                            addPartitionTopicInfo(currentTopicRegistry, topicDirs, partition, topic, consumerThreadId);
                            // record the partition ownership decision
                            partitionOwnershipDecision.put(new Pair<>(topic, partition) , consumerThreadId);
                        }
                    }
                }
            }
            /**
             * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
             * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
             */
            if(reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                logger.info("Updating the cache");
                logger.debug("Partitions per topic cache " + partitionsPerTopicMap);
                logger.debug("Consumers per topic cache " + consumersPerTopicMap);
                topicRegistry = currentTopicRegistry;
                updateFetcher(cluster);
                return true;
            }else {
                return false;
            }
        }

        private void closeFetchersForQueues(Cluster cluster,Map<String,List<KafkaStream<K,V>>> messageStreams,
                                            List<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {

            List<PartitionTopicInfo> allPartitionInfos = new ArrayList<>();
            for(Pool<Integer, PartitionTopicInfo> p : topicRegistry.values()){
                allPartitionInfos.addAll(p.values());
            }
            if(fetcher != null) {
                try{
                    fetcher.stopConnections();
                }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
                clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams);
                logger.info("Committing all offsets after clearing the fetcher queues");
                /**
                 * here, we need to commit offsets before stopping the consumer from returning any more messages
                 * from the current data chunk. Since partition ownership is not yet released, this commit offsets
                 * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
                 * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
                 * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
                 * successfully and the fetchers restart to fetch more data chunks
                 **/
                if (config.autoCommitEnable)
                    commitOffsets();
            }
        }
        private void clearFetcherQueues(List<PartitionTopicInfo> topicInfos, Cluster cluster,
                                        List<BlockingQueue<FetchedDataChunk>>  queuesTobeCleared,
                                        Map<String,List<KafkaStream<K,V>>>  messageStreams) {

            // Clear all but the currently iterated upon chunk in the consumer thread's queue
            for(BlockingQueue<FetchedDataChunk> b:queuesTobeCleared){
                b.clear();
            }
            logger.info("Cleared all relevant queues for this fetcher");

            // Also clear the currently iterated upon chunk in the consumer threads
            if(messageStreams != null){
                for(Map.Entry<String, List<KafkaStream<K,V>>> entry : messageStreams.entrySet()){
                    entry.getValue().forEach(s -> s.clear());
                }
            }
            logger.info("Cleared the data chunks in all the consumer message iterators");

        }
        private void closeFetchers(Cluster cluster,Map<String,List<KafkaStream<K,V>>> messageStreams,
                                    Map<String, Set<String>> relevantTopicThreadIdsMap) {
            List<BlockingQueue<FetchedDataChunk>> queuesToBeCleared = new ArrayList<>();
            // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
            // after this rebalancing attempt
            for (Map.Entry<Pair<String,String>, BlockingQueue<FetchedDataChunk>> entry : topicThreadIdAndQueues.pool().entrySet()) {
                if(!relevantTopicThreadIdsMap.containsKey(entry.getKey().getKey())){
                    queuesToBeCleared.add(entry.getValue());
                }
            }
            closeFetchersForQueues(cluster,messageStreams, queuesToBeCleared);
        }

        private void updateFetcher(Cluster cluster) {
            // update partitions for fetcher
            List<PartitionTopicInfo> allPartitionInfos  = new ArrayList<>();
            for (Pool<Integer, PartitionTopicInfo> partitionInfos : topicRegistry.values())
                for (PartitionTopicInfo partition : partitionInfos.values())
                    allPartitionInfos.add(partition);
            logger.info("Consumer " + consumerIdString + " selected partitions : " +
                    allPartitionInfos.toString());
            if(fetcher != null){
                fetcher.startConnections(allPartitionInfos, cluster);
            }
        }

        private boolean reflectPartitionOwnershipDecision(Map<Pair<String, Integer>, String> partitionOwnershipDecision ){
            List<Pair<String, Integer>> successfullyOwnedPartitions = new ArrayList<>();
            List<Boolean> partitionOwnershipSuccessful =  new ArrayList<>();
            for (Map.Entry<Pair<String, Integer>, String> entry : partitionOwnershipDecision.entrySet()) {
                String topic = entry.getKey().getKey();
                Integer partition = entry.getKey().getValue();
                String consumerThreadId = entry.getValue();
                ZkUtils.ZKGroupTopicDirs topicDirs = new ZkUtils.ZKGroupTopicDirs(group, topic);
                String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition;
                try {
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    logger.info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic);
                    successfullyOwnedPartitions.add(new Pair<>(topic, partition));
                    partitionOwnershipSuccessful.add(true);
                }
                catch (ZkNodeExistsException e){
                    // The node hasn't been deleted by the original owner. So wait a bit and retry.
                    logger.info("waiting for the partition ownership to be deleted: " + partition);
                    partitionOwnershipSuccessful.add(false);
                }
            }
            int hasPartitionOwnershipFailed = partitionOwnershipSuccessful.stream().collect(Collectors.summingInt(new ToIntFunction<Boolean>() {
                @Override
                public int applyAsInt(Boolean decision) {
                    int sum = 0;
                    if(!decision) sum += 1;
                    return sum;
                }
            }));
            /* even if one of the partition ownership attempt has failed, return false */
            if(hasPartitionOwnershipFailed > 0) {
                // remove all paths that we have owned in ZK
                for(Pair<String, Integer> topicAndPartition:successfullyOwnedPartitions){
                    deletePartitionOwnershipFromZK(topicAndPartition.getKey(), topicAndPartition.getValue());
                }
                return false;
            }
            else return true;
        }

        private void addPartitionTopicInfo(Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry,
                                           ZkUtils.ZKGroupTopicDirs topicDirs, int partition,
                                           String topic,String consumerThreadId) throws IOException{
            Pool<Integer, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.get(topic);

            String znode = topicDirs.consumerOffsetDir + "/" + partition;
            String offsetString = ZkUtils.readDataMaybeNull(zkClient, znode).getKey();
            // If first time starting a consumer, set the initial offset based on the config
            long offset = 0L;
            if (offsetString == null) {
                offset = PartitionTopicInfo.InvalidOffset;
            }
            else{
                offset = Long.parseLong(offsetString);
            }
            BlockingQueue<FetchedDataChunk> queue = topicThreadIdAndQueues.get(new Pair<>(topic, consumerThreadId));
            AtomicLong consumedOffset = new AtomicLong(offset);
            AtomicLong fetchedOffset = new AtomicLong(offset);
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,
                    partition,
                    queue,
                    consumedOffset,
                    fetchedOffset,
                    new AtomicInteger(config.fetchMessageMaxBytes),config.clientId);
            partTopicInfoMap.put(partition, partTopicInfo);
            logger.debug(partTopicInfo + " selected new offset " + offset);
        }
    }

    public  class WildcardStreamsHandler<K,V> implements TopicEventHandler<String>{

        public TopicFilter topicFilter;
        public int numStreams;
        public Decoder<K> keyDecoder;
        public Decoder<V> valueDecoder;

        public WildcardStreamsHandler(TopicFilter topicFilter, int numStreams,Decoder<K> keyDecoder,Decoder<V> valueDecoder) throws Exception {
            this.topicFilter = topicFilter;
            this.numStreams = numStreams;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;

            if (messageStreamCreated.getAndSet(true))
                throw new RuntimeException("Each consumer connector can create " +
                        "message streams by filter at most once.");

            for(int i = 0;i < numStreams;i++){
                BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                KafkaStream<K,V> stream = new KafkaStream<K,V>(queue, config.consumerTimeoutMs, keyDecoder,valueDecoder, config.clientId);
                wildcardQueuesAndStreams.add(new Pair<>(queue,stream));
            }
            wildcardTopics =  ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath).stream().filter(topic -> topicFilter.isTopicAllowed(topic)).collect(Collectors.toList());
            wildcardTopicCount = TopicCountFactory.constructTopicCount(consumerIdString, topicFilter, numStreams, zkClient);
            dirs = new ZkUtils.ZKGroupDirs(config.groupId);

            registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount);
            reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);

            if (!topicFilter.requiresTopicEventWatcher()) {
                logger.info("Not creating event watcher for trivial whitelist " + topicFilter);
            }
            else {
                logger.info("Creating topic event watcher for whitelist " + topicFilter);
                wildcardTopicWatcher = new ZookeeperTopicEventWatcher(config, this);

                /*
                 * Topic events will trigger subsequent synced rebalances. Also, the
                 * consumer will get registered only after an allowed topic becomes
                 * available.
                 */
            }
        }

        private List<Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>>> wildcardQueuesAndStreams ;

        // bootstrap with existing topics
        private List<String> wildcardTopics ;

        private TopicCount wildcardTopicCount ;

        ZkUtils.ZKGroupDirs dirs ;

        public void handleTopicEvent(List<String> allTopics){
            logger.debug("Handling topic event");
            List<String> updatedTopics = allTopics.stream().filter(topic -> topicFilter.isTopicAllowed(topic)).collect(Collectors.toList());
            List<String> addedTopics = new ArrayList<>();
            for(String addedTopic : updatedTopics){
                if(!wildcardTopics.contains(addedTopic)) addedTopics.add(addedTopic);
            }
            if (!addedTopics.isEmpty())
                logger.info("Topic event: added topics = %s".format(addedTopics.toString()));
            /*
             * TODO: Deleted topics are interesting (and will not be a concern until
             * 0.8 release). We may need to remove these topics from the rebalance
             * listener's map in reinitializeConsumer.
             */
            List<String> deletedTopics = new ArrayList<>();
            for(String deletedTopic:wildcardTopics){
                if(!updatedTopics.contains(deletedTopic)) deletedTopics.add(deletedTopic);
            }
            if (!deletedTopics.isEmpty())
                logger.info("Topic event: deleted topics = %s"
                        .format(deletedTopics.toString()));

            wildcardTopics = updatedTopics;
            logger.info("Topics to consume = %s".format(wildcardTopics.toString()));

            if (!addedTopics.isEmpty() || !deletedTopics.isEmpty()) {
                try{
                    reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
                }catch (InterruptedException e){
                    logger.error("WildcardStreamsHandler reinitializeConsumer Error:",e);
                    throw  new RuntimeException(e.getMessage());
                }
            }
        }

        public List<KafkaStream<K,V>> streams() {
            List<KafkaStream<K,V>> streams = new ArrayList<>();
            for (Pair<BlockingQueue<FetchedDataChunk>,KafkaStream<K,V>> stream : wildcardQueuesAndStreams) {
                streams.add(stream.getValue());
            }
            return streams;
        }
    }
    class ZKTopicPartitionChangeListener implements IZkDataListener {

        ZKRebalancerListener loadBalancerListener;

        public ZKTopicPartitionChangeListener(ZKRebalancerListener loadBalancerListener) {
            this.loadBalancerListener = loadBalancerListener;
        }

        public  void handleDataChange(String dataPath, Object data) throws Exception{
            try {
                logger.info("Topic info for path " + dataPath + " changed to " + data.toString() + ", triggering rebalance");
                // explicitly trigger load balancing for this consumer
                loadBalancerListener.syncedRebalance();

                // There is no need to re-subscribe the watcher since it will be automatically
                // re-registered upon firing of this event by zkClient
            } catch (Throwable e){
                logger.error("Error while handling topic partition change for data path " + dataPath, e );
            }
        }

        public  void handleDataDeleted(String dataPath) throws Exception{
            // TODO: This need to be implemented when we support delete topic
            logger.warn("Topic for path " + dataPath + " gets deleted, which should not happen at this time");
        }
    }
}

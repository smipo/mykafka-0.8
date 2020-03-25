package kafka.producer;

import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ZKBrokerPartitionInfo implements BrokerPartitionInfo{

    private static Logger logger = Logger.getLogger(ZKBrokerPartitionInfo.class);

    ZkUtils.ZKConfig config;
    List<Three<Integer, String, Integer>> producerCbk;

    public ZKBrokerPartitionInfo(ZkUtils.ZKConfig config, List<Three<Integer, String, Integer>> producerCbk) {
        this.config = config;
        this.producerCbk = producerCbk;

        this.zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                new ZKStringSerializer());
        this.topicBrokerPartitions = getZKTopicPartitionInfo();
        this.allBrokers = getZKBrokerInfo();

        // register listener for change of topics to keep topicsBrokerPartitions updated
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, brokerTopicsListener);

        // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
        for(String topic : topicBrokerPartitions.keySet()){
            zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, brokerTopicsListener);
            logger.debug("Registering listener on path: " + ZkUtils.BrokerTopicsPath + "/" + topic);
        }

        // register listener for new broker
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerTopicsListener);

        // register listener for session expired event
        zkClient.subscribeStateChanges(new ZKSessionExpirationListener(brokerTopicsListener));

        brokerTopicsListener = new BrokerTopicsListener(topicBrokerPartitions, allBrokers);
    }

    private Object zkWatcherLock = new Object();
    private ZkClient zkClient;
    // maintain a map from topic -> list of (broker, num_partitions) from zookeeper
    private Map<String, SortedSet<Partition>> topicBrokerPartitions;
    // maintain a map from broker id to the corresponding Broker object
    private  Map<Integer, Broker> allBrokers ;

    // use just the brokerTopicsListener for all watchers
    private BrokerTopicsListener brokerTopicsListener ;



    /**
     * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
     * specified
     * @param topic the topic to which the brokers have registered
     * @param brokerList the list of brokers for which the partitions info is to be generated
     * @return a sequence of (brokerId, numPartitions) for brokers in brokerList
     */
    private static  SortedSet<Partition>  getBrokerPartitions(ZkClient zkClient, String topic,List<Integer> brokerList) {
        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;

        List<Pair<Integer,Integer>> brokerPartitions = new ArrayList<>();
        for(Integer bid:brokerList){
            Integer numPartition = Integer.parseInt(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
            brokerPartitions.add(new Pair<>(bid,numPartition));
        }
        brokerPartitions.sort((brokerPartition1, brokerPartition2) -> brokerPartition1.getKey().compareTo(brokerPartition2.getKey()));
        SortedSet<Partition> brokerParts = Utils.getTreeSetSet();
        for(Pair<Integer,Integer> bp:brokerPartitions){
            for(int i = 0 ;i < bp.getValue();i++) {
                Partition bidPid = new Partition(bp.getKey(), i);
                brokerParts.add(bidPid);
            }
        }
        return brokerParts;
    }
    /**
     * Return a sequence of (brokerId, numPartitions)
     * @param topic the topic for which this information is to be returned
     * @return a sequence of (brokerId, numPartitions). Returns a zero-length
     * sequence if no brokers are available.
     */
    public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
        synchronized(zkWatcherLock) {
            SortedSet<Partition> brokerPartitions = topicBrokerPartitions.get(topic);
            SortedSet<Partition> numBrokerPartitions = Utils.getTreeSetSet();
            if(brokerPartitions == null){ // no brokers currently registered for this topic. Find the list of all brokers in the cluster.
                numBrokerPartitions = bootstrapWithExistingBrokers(topic);
                topicBrokerPartitions.put (topic , numBrokerPartitions);
            } else if(brokerPartitions.size() == 0){ // no brokers currently registered for this topic. Find the list of all brokers in the cluster.
                numBrokerPartitions = bootstrapWithExistingBrokers(topic);
                topicBrokerPartitions.put (topic , numBrokerPartitions);
            }else{
                numBrokerPartitions.addAll(brokerPartitions);
            }
            return numBrokerPartitions;
        }
    }
    /**
     * Generate the host and port information for the broker identified
     * by the given broker id
     * @param brokerId the broker for which the info is to be returned
     * @return host and port of brokerId
     */
    public Broker getBrokerInfo(int brokerId)  {
        synchronized(zkWatcherLock) {
            return allBrokers.get(brokerId);
        }
    }
    /**
     * Generate a mapping from broker id to the host and port for all brokers
     * @return mapping from id to host and port of all brokers
     */
    public Map<Integer, Broker> getAllBrokerInfo(){
        return allBrokers;
    }

    public void close(){
        zkClient.close();
    }

    public void updateInfo () {
        synchronized(zkWatcherLock) {
            topicBrokerPartitions = getZKTopicPartitionInfo();
            allBrokers = getZKBrokerInfo();
        }
    }

    private SortedSet<Partition> bootstrapWithExistingBrokers(String topic) {
        logger.debug("Currently, no brokers are registered under topic: " + topic);
        logger.debug("Bootstrapping topic: " + topic + " with available brokers in the cluster with default " +
                "number of partitions = 1");
        List<String> allBrokersIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        logger.trace("List of all brokers currently registered in zookeeper = " + allBrokersIds.toString());
        // since we do not have the in formation about number of partitions on these brokers, just assume single partition
        // i.e. pick partition 0 from each broker as a candidate
        TreeSet<Partition> numBrokerPartitions = Utils.getTreeSetSet();
        for(String brokerId:allBrokersIds){
            numBrokerPartitions.add(new Partition(Integer.parseInt(brokerId), 0));
        }
        // add the rest of the available brokers with default 1 partition for this topic, so all of the brokers
        // participate in hosting this topic.
        logger.debug("Adding following broker id, partition id for NEW topic: " + topic + "=" + numBrokerPartitions.toString());
        return numBrokerPartitions;
    }

    /**
     * Generate a sequence of (brokerId, numPartitions) for all topics
     * registered in zookeeper
     * @return a mapping from topic to sequence of (brokerId, numPartitions)
     */
    private Map<String, SortedSet<Partition>> getZKTopicPartitionInfo() {
        Map<String, SortedSet<Partition>> brokerPartitionsPerTopic = new HashMap<>();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath);
        for(String topic:topics){
            // find the number of broker partitions registered for this topic
            String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
            List<Pair<Integer,Integer>> brokerPartitions = new ArrayList<>();
            for(String bid:brokerList){
                Integer numPartition = Integer.parseInt(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
                brokerPartitions.add(new Pair<>(Integer.parseInt(bid),numPartition));
            }
            brokerPartitions.sort((brokerPartition1, brokerPartition2) -> brokerPartition1.getKey().compareTo(brokerPartition2.getKey()));
            logger.debug("Broker ids and # of partitions on each for topic: " + topic + " = " + brokerPartitions.toString());

            SortedSet<Partition> brokerParts = Utils.getTreeSetSet();
            for(Pair<Integer,Integer> bp:brokerPartitions){
                for(int i = 0 ;i < bp.getValue();i++) {
                    Partition bidPid = new Partition(bp.getKey(), i);
                    brokerParts.add(bidPid);
                }
            }
            brokerPartitionsPerTopic.put(topic , brokerParts);
            logger.debug("Sorted list of broker ids and partition ids on each for topic: " + topic + " = " + brokerParts.toString());
        }
        return brokerPartitionsPerTopic;
    }

    /**
     * Generate a mapping from broker id to (brokerId, numPartitions) for all brokers
     * registered in zookeeper
     * @return a mapping from brokerId to (host, port)
     */
    private  Map<Integer, Broker> getZKBrokerInfo() {
        Map<Integer, Broker> brokers = new HashMap<>();
        List<String> allBrokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        for(String bid:allBrokerIds){
            int brokerId = Integer.parseInt(bid);
            String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid);
            brokers.put(brokerId,Broker.createBroker(brokerId, brokerInfo));
        }
        return brokers;
    }
    /**
     * Listens to new broker registrations under a particular topic, in zookeeper and
     * keeps the related data structures updated
     */
    public class BrokerTopicsListener implements IZkChildListener {

        Map<String, SortedSet<Partition>> originalBrokerTopicsPartitionsMap;
        Map<Integer, Broker> originalBrokerIdMap;


        private Map<String, SortedSet<Partition>> oldBrokerTopicPartitionsMap = new ConcurrentHashMap<>();
        private Map<Integer, Broker> oldBrokerIdMap  = new ConcurrentHashMap<>();

        public BrokerTopicsListener(Map<String, SortedSet<Partition>> originalBrokerTopicsPartitionsMap,
                                    Map<Integer, Broker> originalBrokerIdMap) {
            this.originalBrokerTopicsPartitionsMap = originalBrokerTopicsPartitionsMap;
            this.originalBrokerIdMap = originalBrokerIdMap;

            this.oldBrokerTopicPartitionsMap.putAll(originalBrokerTopicsPartitionsMap);
            this.oldBrokerIdMap.putAll(originalBrokerIdMap);

            logger.debug("[BrokerTopicsListener] Creating broker topics listener to watch the following paths - \n" +
                    "/broker/topics, /broker/topics/topic, /broker/ids");
            logger.debug("[BrokerTopicsListener] Initialized this broker topics listener with initial mapping of broker id to " +
                    "partition id per topic with " + oldBrokerTopicPartitionsMap.toString());
        }

        public void handleChildChange(String parentPath, List<String> currentChildren)  throws Exception{
            List<String> curChilds = new ArrayList<>();
            if (currentChildren != null) curChilds = currentChildren;

            synchronized(zkWatcherLock) {
                logger.trace("Watcher fired for path: " + parentPath + " with change " + curChilds.toString());
                if("/brokers/topics".equals(parentPath)){ // this is a watcher for /broker/topics path
                    List<String> updatedTopics = curChilds;
                    logger.debug("[BrokerTopicsListener] List of topics changed at " + parentPath + " Updated topics -> " +
                            curChilds.toString());
                    logger.debug("[BrokerTopicsListener] Old list of topics: " + oldBrokerTopicPartitionsMap.keySet().toString());
                    logger.debug("[BrokerTopicsListener] Updated list of topics: " + updatedTopics.toString());
                    List<String> newTopics = new ArrayList<>();
                    for(String newTopic:updatedTopics){
                        if(!oldBrokerTopicPartitionsMap.containsKey(newTopic)) newTopics.add(newTopic);
                    }
                    logger.debug("[BrokerTopicsListener] List of newly registered topics: " + newTopics.toString());
                    for(String topic:newTopics){
                        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
                        List<String>  brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
                        processNewBrokerInExistingTopic(topic, brokerList);
                        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic,
                                brokerTopicsListener);
                    }
                }else if("/brokers/ids".equals(parentPath)){ // this is a watcher for /broker/ids path
                    logger.debug("[BrokerTopicsListener] List of brokers changed in the Kafka cluster " + parentPath +
                            "\t Currently registered list of brokers -> " + curChilds.toString());
                    processBrokerChange(parentPath, curChilds);
                }else{
                    String[] pathSplits = parentPath.split("/");
                    String topic = pathSplits[pathSplits.length - 1];
                    if (pathSplits.length == 4 && pathSplits[2].equals("topics")) {
                        logger. debug("[BrokerTopicsListener] List of brokers changed at " + parentPath + "\t Currently registered " +
                                " list of brokers -> " + curChilds.toString() + " for topic -> " + topic);
                        processNewBrokerInExistingTopic(topic, curChilds);
                    }
                }
            }
            // update the data structures tracking older state values
            oldBrokerTopicPartitionsMap.putAll(topicBrokerPartitions);
            oldBrokerIdMap.putAll(allBrokers);
        }

        public void processBrokerChange(String parentPath, List<String> curChilds) {
            if (parentPath.equals(ZkUtils.BrokerIdsPath)) {
                Set<Integer> updatedBrokerList = new HashSet<>();
                for(String curChild:curChilds){
                    updatedBrokerList.add(Integer.parseInt(curChild));
                }
                List<Integer> newBrokers = new ArrayList<>();
                Iterator<Integer>  updatedBrokerIterator = updatedBrokerList.iterator();
                while (updatedBrokerIterator.hasNext()){
                    Integer newBrokerId = updatedBrokerIterator.next();
                    if(!oldBrokerIdMap.containsKey(newBrokerId)) newBrokers.add(newBrokerId);
                }
                logger.debug("[BrokerTopicsListener] List of newly registered brokers: " + newBrokers.toString());
                for(Integer bid:newBrokers){
                    String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid);
                    String[] brokerHostPort = brokerInfo.split(":");
                    allBrokers.put (bid , new Broker(bid, brokerHostPort[1], brokerHostPort[1], Integer.parseInt(brokerHostPort[2])));
                    logger.debug("[BrokerTopicsListener] Invoking the callback for broker: " + bid);
                    producerCbk.add(new Three(bid, brokerHostPort[1], brokerHostPort[2]));
                }
                // remove dead brokers from the in memory list of live brokers
                List<Integer> deadBrokers = new ArrayList<>();
                for(Integer oldBrokerId:oldBrokerIdMap.keySet()){
                    if(!updatedBrokerList.contains(oldBrokerId)){
                        deadBrokers.add(oldBrokerId);
                    }
                }
                logger.debug("[BrokerTopicsListener] Deleting broker ids for dead brokers: " + deadBrokers.toString());
                for(Integer bid:deadBrokers){
                    allBrokers.remove(bid);
                    for (Map.Entry<String, SortedSet<Partition>> entry : topicBrokerPartitions.entrySet()) {
                        SortedSet<Partition> oldBrokerPartition = entry.getValue();
                        Iterator<Partition> oldBrokerIterator = oldBrokerPartition.iterator();
                        while (oldBrokerIterator.hasNext()){
                            Partition oldPartition = oldBrokerIterator.next();
                            if(oldPartition.brokerId() == bid){
                                oldBrokerIterator.remove();
                                logger.debug("[BrokerTopicsListener] Removing dead broker ids for topic: " + entry.getKey() + "\t " +
                                        "Updated list of broker id, partition id = " + oldPartition.toString());
                            }
                        }
                    }
                }
            }
        }

        /**
         * Generate the updated mapping of (brokerId, numPartitions) for the new list of brokers
         * registered under some topic
         * @param parentPath the path of the topic under which the brokers have changed
         * @param curChilds the list of changed brokers
         */
        public void processNewBrokerInExistingTopic(String topic, List<String> curChilds){
            List<Integer> updatedBrokerList = new ArrayList<>();
            for(String curChild:curChilds){
                updatedBrokerList.add(Integer.parseInt(curChild));
            }
            SortedSet<Partition> updatedBrokerParts = getBrokerPartitions(zkClient, topic, updatedBrokerList);
            logger.debug("[BrokerTopicsListener] Currently registered list of brokers for topic: " + topic + " are " +
                    curChilds.toString());
            // update the number of partitions on existing brokers
            SortedSet<Partition> mergedBrokerParts = Utils.getTreeSetSet();
            mergedBrokerParts.addAll(updatedBrokerParts);
            SortedSet<Partition> oldBrokerParts = topicBrokerPartitions.get(topic);
            if(oldBrokerParts != null){
                logger.debug("[BrokerTopicsListener] Unregistered list of brokers for topic: " + topic + " are " +
                        oldBrokerParts.toString());
                mergedBrokerParts.addAll(oldBrokerParts);
                mergedBrokerParts.addAll(updatedBrokerParts);
            }
            // keep only brokers that are alive
            for (Partition bp:mergedBrokerParts) {
                if(allBrokers.containsKey(bp.brokerId())) mergedBrokerParts.remove(bp);
            }
            topicBrokerPartitions.put(topic , mergedBrokerParts);
            logger.debug("[BrokerTopicsListener] List of broker partitions for topic: " + topic + " are " +
                    mergedBrokerParts.toString());
        }

        public void resetState(){
            logger.trace("[BrokerTopicsListener] Before reseting broker topic partitions state " +
                    oldBrokerTopicPartitionsMap.toString());
            oldBrokerTopicPartitionsMap.putAll(topicBrokerPartitions);
            logger.debug("[BrokerTopicsListener] After reseting broker topic partitions state " +
                    oldBrokerTopicPartitionsMap.toString());
            logger. trace("[BrokerTopicsListener] Before reseting broker id map state " + oldBrokerIdMap.toString());
            oldBrokerIdMap.putAll(allBrokers);
            logger.debug("[BrokerTopicsListener] After reseting broker id map state " + oldBrokerIdMap.toString());
        }
    }
    public class ZKSessionExpirationListener implements IZkStateListener {

        BrokerTopicsListener brokerTopicsListener;

        public ZKSessionExpirationListener(BrokerTopicsListener brokerTopicsListener){
            this.brokerTopicsListener = brokerTopicsListener;
        }

        public void handleStateChanged(Watcher.Event.KeeperState var1) throws Exception{
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
             *  connection for us.
             */
            logger.info("ZK expired; release old list of broker partitions for topics ");
            topicBrokerPartitions = getZKTopicPartitionInfo();
            allBrokers = getZKBrokerInfo();
            brokerTopicsListener.resetState();

            // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
            // NOTE: this is probably not required here. Since when we read from getZKTopicPartitionInfo() above,
            // it automatically recreates the watchers there itself
            topicBrokerPartitions.keySet().stream().forEach(topic -> zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic,
                    brokerTopicsListener));
            // there is no need to re-register other listeners as they are listening on the child changes of
            // permanent nodes
        }

        public void handleSessionEstablishmentError(Throwable var1) throws Exception{
            // do nothing,
        }

    }

}
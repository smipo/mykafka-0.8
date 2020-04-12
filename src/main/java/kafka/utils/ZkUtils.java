package kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.api.LeaderAndIsrRequest;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.AdministrationException;
import kafka.common.KafkaException;
import kafka.common.NoEpochForPartitionException;
import kafka.common.TopicAndPartition;
import kafka.consumer.TopicCount;
import kafka.consumer.TopicCountFactory;
import kafka.controller.KafkaController;
import kafka.controller.LeaderIsrAndControllerEpoch;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

public class ZkUtils {

    private static Logger logger = Logger.getLogger(ZkUtils.class);


    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";

    public static final String ControllerPath = "/controller";
    public static final String ControllerEpochPath = "/controller_epoch";
    public static final String ReassignPartitionsPath = "/admin/reassign_partitions";
    public static final String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";


    public static String getTopicPath(String topic){
        return BrokerTopicsPath + "/" + topic;
    }

    public static String getTopicPartitionsPath(String topic){
        return getTopicPath(topic) + "/partitions";
    }

    public static int getController(ZkClient zkClient) {
        String controller = readDataMaybeNull(zkClient, ControllerPath).getKey();
        if(controller == null){
            throw new KafkaException("Controller doesn't exist");
        }
        KafkaController.parseControllerId(controller);
    }

    public static String getTopicPartitionPath(String topic, int partitionId){
        return getTopicPartitionsPath(topic) + "/" + partitionId;
    }

    public static String getTopicPartitionLeaderAndIsrPath(String topic, int partitionId){
        return getTopicPartitionPath(topic, partitionId) + "/" + "state";
    }

    public static List<Integer> getSortedBrokerList(ZkClient zkClient){
        List<String> brokerIds = ZkUtils.getChildren(zkClient, BrokerIdsPath);
        return brokerIds.stream().map(Integer::parseInt).collect(Collectors.toList());
    }

    public static List<Broker> getAllBrokersInCluster(ZkClient zkClient) {
        List<String> brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        List<Broker> brokers = new ArrayList<>();
        for(String brokerId:brokerIds){
            Broker broker = getBrokerInfo(zkClient, Integer.parseInt(brokerId));
            if(broker != null){
                brokers.add(broker);
            }
        }
        return brokers;
    }

    public static LeaderIsrAndControllerEpoch getLeaderIsrAndEpochForPartition(ZkClient zkClient, String topic, int partition)throws IOException{
        String leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition);
        Pair<String,Stat> leaderAndIsrInfo = readDataMaybeNull(zkClient, leaderAndIsrPath);
        String leaderAndIsrOpt = leaderAndIsrInfo.getKey();
        Stat stat = leaderAndIsrInfo.getValue();
        if(leaderAndIsrOpt != null){
            return parseLeaderAndIsr(leaderAndIsrOpt, topic, partition, stat);
        }
        return null;
    }

    public static LeaderAndIsrRequest.LeaderAndIsr getLeaderAndIsrForPartition(ZkClient zkClient, String topic, int partition) throws IOException{
        LeaderIsrAndControllerEpoch isrAndControllerEpoch = getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
        if(isrAndControllerEpoch != null){
            return isrAndControllerEpoch.leaderAndIsr;
        }
        return null;
    }

    public static LeaderIsrAndControllerEpoch parseLeaderAndIsr(String leaderAndIsrStr, String topic, int partition, Stat stat) throws IOException {
        if(leaderAndIsrStr == null || leaderAndIsrStr.isEmpty()){
            return null;
        }
        Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(leaderAndIsrStr);
        int leader = Integer.parseInt(leaderIsrAndEpochInfo.get("leader").toString());
        int epoch = Integer.parseInt(leaderIsrAndEpochInfo.get("leader_epoch").toString());
        List<Integer> isr = ( List<Integer>)leaderIsrAndEpochInfo.get("isr");
        int controllerEpoch = Integer.parseInt(leaderIsrAndEpochInfo.get("controller_epoch").toString());
        int zkPathVersion = stat.getVersion();
        logger.debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for partition [%s,%d]".format(leader + "", epoch,
                isr , zkPathVersion, topic, partition));
        return new LeaderIsrAndControllerEpoch(new LeaderAndIsrRequest.LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch);
    }

    public static Integer getLeaderForPartition(ZkClient zkClient,String topic, int partition)throws IOException{
        String leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).getKey();
        if(leaderAndIsrOpt != null){
            Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(leaderAndIsrOpt);
            String res = leaderIsrAndEpochInfo.get("leader") == null?null:leaderIsrAndEpochInfo.get("leader").toString();
            if(res == null) return null;
            return Integer.parseInt(res);
        }
        return null;
    }

    /**
     * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
     * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
     * other broker will retry becoming leader with the same new epoch value.
     */
    public static int getEpochForPartition(ZkClient zkClient,String topic, int partition) throws IOException{
        String leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).getKey();
        if(leaderAndIsrOpt == null){
            throw new NoEpochForPartitionException("No epoch, ISR path for partition [%s,%d] is empty"
                    .format(topic, partition));
        }
        Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(leaderAndIsrOpt);
        String res = leaderIsrAndEpochInfo.get("leader_epoch") == null?null:leaderIsrAndEpochInfo.get("leader_epoch").toString();
        if(res == null) throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid".format(topic, partition));
        return Integer.parseInt(res);
    }

    /**
     * Gets the in-sync replicas (ISR) for a specific topic and partition
     */
    public static List<Integer> getInSyncReplicasForPartition(ZkClient zkClient,String topic, int partition) throws IOException{
        String leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).getKey();
        if(leaderAndIsrOpt == null){
            return new ArrayList<>();
        }
        Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(leaderAndIsrOpt);
        return  leaderIsrAndEpochInfo.get("isr") == null?new ArrayList<>(): (List<Integer>)leaderIsrAndEpochInfo.get("isr");
    }

    /**
     * Gets the assigned replicas (AR) for a specific topic and partition
     */
    public static List<Integer> getReplicasForPartition(ZkClient zkClient,String topic, int partition) throws IOException{
        String jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).getKey();
        if(jsonPartitionMapOpt == null){
            return new ArrayList<>();
        }
        Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(jsonPartitionMapOpt);
        Object partitions = leaderIsrAndEpochInfo.get("partitions");
        if(partitions == null){
            return new ArrayList<>();
        }
        Map<String,List<Integer>> replicaMap = (Map<String,List<Integer>>)partitions;
        List<Integer> replicas = replicaMap.get(String.valueOf(partition));
        if(replicas == null){
            return new ArrayList<>();
        }
        return replicas;
    }

    public static boolean isPartitionOnBroker(ZkClient zkClient,String topic, int partition,int  brokerId)throws IOException{
        List<Integer> replicas = getReplicasForPartition(zkClient, topic, partition);
        logger.debug("The list of replicas for partition [%s,%d] is %s".format(topic, partition, replicas));
        return replicas.contains(String.valueOf(brokerId));
    }

    public static void registerBrokerInZk(ZkClient zkClient,int id,String host, int port, int timeout, int jmxPort) {
        String brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id;
        String timestamp = "\"" + System.currentTimeMillis() + "\"";
        Map<String, String> jsonDataMap1 = new HashMap<>();
        jsonDataMap1.put("host",host);
        Map<String, String> jsonDataMap2 = new HashMap<>();
        jsonDataMap2.put("version","1");
        jsonDataMap2.put("jmx_port",String.valueOf(jmxPort));
        jsonDataMap2.put("port",String.valueOf(port));
        jsonDataMap2.put("timestamp",timestamp);
        List<String> l1 = Utils.mapToJsonFields(jsonDataMap1, true);
        List<String> l2 = Utils.mapToJsonFields(jsonDataMap2, false);
        l1.addAll(l2);
        String brokerInfo = Utils.mergeJsonFields(l1);
        Broker expectedBroker = new Broker(id, host, port);
        try {
            createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
                    timeout);

        } catch (ZkNodeExistsException | InterruptedException e){
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                    + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                    + "else you have shutdown this broker and restarted it faster than the zookeeper "
                    + "timeout so it appears to be re-registering.");
        }
        logger.info("Registered broker %d at path %s with address %s:%d.".format(String.valueOf(id), brokerIdPath, host, port));
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistsException if node already exists.
     * Handles the following ZK session timeout bug:
     *
     * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
     *
     * Upon receiving a NodeExistsException, read the data from the conflicted path and
     * trigger the checker function comparing the read data and the expected data,
     * If the checker function returns true then the above bug might be encountered, back off and retry;
     * otherwise re-throw the exception
     */
    public static void createEphemeralPathExpectConflictHandleZKBug(ZkClient zkClient,String path, String data, Object expectedCallerData,  int backoffTime) throws InterruptedException {
        while (true) {
            try {
                createEphemeralPathExpectConflict(zkClient, path, data);
                return;
            } catch (ZkNodeExistsException e){
                // An ephemeral node may still exist even after its corresponding session has expired
                // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
                // and hence the write succeeds without ZkNodeExistsException
                String r = ZkUtils.readDataMaybeNull(zkClient, path).getKey();
                if(r != null) {
                    Broker b = (Broker)expectedCallerData;
                    if (Broker.createBroker(b.id(), r).equals(b)){
                        logger.info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data, path)
                                + "hence I will backoff for this node to be deleted by Zookeeper and retry");

                        Thread.sleep(backoffTime);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }
    public static String getConsumerPartitionOwnerPath(String group,String topic,int partition) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
        return topicDirs.consumerOwnerDir + "/" + partition;
    }

    public static String leaderAndIsrZkData(LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr, int controllerEpoch) {
        String isrInfo = Utils.seqToJson(leaderAndIsr.isr.stream().map(x -> String.valueOf(x)).collect(Collectors.toList()), false);
        Map<String, String> jsonDataMap = new HashMap<>();
        jsonDataMap.put("version","1");
        jsonDataMap.put("leader",String.valueOf(leaderAndIsr.leader));
        jsonDataMap.put("leader_epoch",String.valueOf(leaderAndIsr.leaderEpoch));
        jsonDataMap.put("controller_epoch",String.valueOf(controllerEpoch));
        jsonDataMap.put("isr",isrInfo);
        return Utils.mapToJson(jsonDataMap, false);
    }

    /**
     * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
     * or throws an exception if the broker dies before the query to zookeeper finishes
     * @param brokerId The broker id
     * @param zkClient The zookeeper client connection
     * @return An optional Broker object encapsulating the broker metadata
     */
    public static Broker getBrokerInfo(ZkClient zkClient,int brokerId) {
        String brokerInfo = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId).getKey();
        if(brokerInfo != null){
            return Broker.createBroker(brokerId, brokerInfo);
        }
        return null;
    }

    /**
     * Get JSON partition to replica map from zookeeper.
     */
    public static String replicaAssignmentZkdata(Map<String,List<Integer>> map) throws JsonProcessingException {
        String jsonReplicaAssignmentMap = JacksonUtils.objToJson(map);
        Map<String,String> maps = new HashMap<>();
        maps.put("version","1");
        maps.put("partitions",jsonReplicaAssignmentMap);
        return Utils.mapToJson(maps, false);
    }

    /**
     *  make sure a persistent path exists in ZK. Create the path if not exist.
     */
    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path))
            client.createPersistent(path, true); // won't throw NoNodeException or NodeExistsException
    }

    /**
     *  create the parent path
     */
    private static void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0)
            client.createPersistent(parentDir, true);
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     */
    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        }
        catch(ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistException if node already exists.
     */
    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data){
        try {
            createEphemeralPath(client, path, data);
        }
        catch (ZkNodeExistsException e){
            // this can happen when there is connection loss; make sure the data is what we intend to write
            String storedData = readData(client, path);
            if (storedData == null || storedData != data) {
                logger.info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                throw e;
            }
            else {
                // otherwise, the creation succeeded, return normally
                logger.info(path + " exists with value " + data + " during connection loss; this is ok");
            }
        }
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public static void updatePersistentPath(ZkClient client, String path, String data){
        try {
            client.writeData(path, data);
        }catch (ZkNoNodeException e){
            createParentPath(client, path);
            try {
                client.createPersistent(path, data);
            }
            catch (ZkNodeExistsException e1){
                client.writeData(path, data);
            }
        }
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public void updateEphemeralPath(ZkClient client, String path, String data){
        try {
            client.writeData(path, data);
        } catch(ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    public static void deletePath(ZkClient client, String path) {
        try {
            client.delete(path);
        } catch (ZkNoNodeException e){
            // this can happen during a connection loss event, return normally
            logger.info(path + " deleted during connection loss; this is ok");
        }
    }

    public static void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch (ZkNoNodeException e){
            // this can happen during a connection loss event, return normally
            logger.info(path + " deleted during connection loss; this is ok");
        }
    }

    public static String readData(ZkClient client, String path){
        return client.readData(path);
    }

    public static Pair<String,Stat> readDataMaybeNull(ZkClient client, String path){
        Stat stat = new Stat();
        return new Pair<>(client.readData(path, stat),stat);
    }

    public static List<String>  getChildren(ZkClient client, String path){
        // triggers implicit conversion from java list to scala Seq
        return client.getChildren(path);
    }

    public static List<String> getChildrenParentMayNotExist(ZkClient client,String path) {
        // triggers implicit conversion from java list to scala Seq
        try {
            return client.getChildren(path);
        }catch (ZkNoNodeException e){
            return null;
        }
    }

    /**
     * Check if the given path exists
     */
    public static boolean pathExists(ZkClient client, String path){
        return client.exists(path);
    }

    public static String getLastPart(String path ) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static  Cluster getCluster(ZkClient zkClient)   {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            String brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node);
            cluster.add(Broker.createBroker(Integer.parseInt(node), brokerZKString));
        }
        return cluster;
    }

    public static Map<String, Map<Integer,List<Integer>>> getPartitionsForTopics(ZkClient zkClient, Collection<String> topics) throws JsonProcessingException {
        Map<String, Map<Integer,List<Integer>>> ret = new HashMap<>();
        for (String topic : topics) {
            Map<Integer,List<Integer>> partitionMap = new HashMap<>();
            String jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).getKey();
            if(jsonPartitionMapOpt == null){
                ret.put(topic,partitionMap);
                continue;
            }
            Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(jsonPartitionMapOpt);
            Object partitions = leaderIsrAndEpochInfo.get("partitions");
            if(partitions == null){
                ret.put(topic,partitionMap);
                continue;
            }
            for(Map.Entry<String, Object> entry : leaderIsrAndEpochInfo.entrySet()){
                partitionMap.put(Integer.parseInt(entry.getKey()),(List<Integer>)entry.getValue());
                ret.put(topic,partitionMap);
            }
        }
        return ret;
    }


    public static void deletePartition(ZkClient zkClient, int brokerId, String topic) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        zkClient.delete(brokerIdPath);
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        zkClient.delete(brokerPartTopicPath);
    }

    public static List<String> getConsumersInGroup(ZkClient zkClient,String group) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        return getChildren(zkClient, dirs.consumerRegistryDir);
    }
    public static Map<String, TopicCount> getConsumerTopicMaps(ZkClient zkClient,String group) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        List<String> consumersInGroup = getConsumersInGroup(zkClient, group);
        Map<String, TopicCount> consumerIdTopicMap = new HashMap<>();
        for(String consumerId:consumersInGroup){
            TopicCount topicCount = TopicCountFactory.constructTopicCount(consumerId,
                    ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId), zkClient);
            consumerIdTopicMap.put(consumerId,topicCount);
        }
        return consumerIdTopicMap;
    }
    public static Map<TopicAndPartition,List<Integer>> getReplicaAssignmentForTopics(ZkClient zkClient, List<String> topics) throws IOException{
        Map<TopicAndPartition,List<Integer>> ret = new HashMap<>();
        for(String topic:topics){
            String jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).getKey();
            if(jsonPartitionMapOpt != null){
                Map<String,Object> leaderIsrAndEpochInfo = JacksonUtils.strToMap(jsonPartitionMapOpt);
                Object obj = leaderIsrAndEpochInfo.get("partitions");
                if(obj != null){
                    Map<String, List<Integer>> replicaMap = (Map<String, List<Integer>>)obj;
                    for(Map.Entry<String, List<Integer>> entry : replicaMap.entrySet()){
                        ret.put(new TopicAndPartition(topic, Integer.parseInt(entry.getKey())), entry.getValue());
                        logger.debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, entry.getKey(), entry.getValue().toString()));

                    }
                }
            }
        }
        return ret;
    }

    public static Map<String, List<String>> getConsumersPerTopic(ZkClient zkClient,String group)  {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
        Map<String, List<String>> consumersPerTopicMap = new HashMap<>();
        for(String consumer:consumers){
            TopicCount topicCount = TopicCountFactory.constructTopicCount(group, consumer, zkClient);
            Iterator<Map.Entry<String, Set<String>>> entries = topicCount.getConsumerThreadIdsPerTopic().entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Set<String>> entry = entries.next();
                String topic = entry.getKey();
                Set<String> consumerThreadIdSet = entry.getValue();
                for (String consumerThreadId : consumerThreadIdSet){
                    List<String> consumerThreadIdList = consumersPerTopicMap.get(topic);
                    if(consumerThreadIdList == null){
                        consumerThreadIdList = new ArrayList<>();
                        consumersPerTopicMap.put(topic,consumerThreadIdList);
                    }
                    consumerThreadIdList.add(consumerThreadId);
                }
            }
        }
        //todo 有排序删除了
        return consumersPerTopicMap;
    }

    public static Map<String,Map<Integer, List<Integer>>> getPartitionAssignmentForTopics(ZkClient zkClient, List<String> topics) throws JsonProcessingException {
        Map<String,Map<Integer, List<Integer>>> ret = new HashMap<>();
        for(String topic:topics){
            String  jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).getKey();
            if(jsonPartitionMapOpt == null || jsonPartitionMapOpt.isEmpty()){
                return new HashMap<>();
            }
            Map<String,Object> replicaMap = JacksonUtils.strToMap(jsonPartitionMapOpt);
            Object obj = replicaMap.get("partitions");
            if(obj == null){
                return new HashMap<>();
            }
            Map<Integer, List<Integer>> valueMap = new HashMap<>();
            Map<String, List<Integer>> m1 = (Map<String, List<Integer>>)obj;
            m1.forEach((k,v)->valueMap.put(Integer.parseInt(k),v));
            ret.put(topic,valueMap);
            logger.debug("Partition map for /brokers/topics/%s is %s".format(topic, ret.toString()));
        }
        return ret;
    }

    public static List<Pair<String,Integer>> getPartitionsAssignedToBroker(ZkClient zkClient, List<String> topics,int brokerId) throws JsonProcessingException {
        List<Pair<String,Integer>> res = new ArrayList<>();
        Map<String,Map<Integer, List<Integer>>> topicsAndPartitions = getPartitionAssignmentForTopics(zkClient, topics);
        for (Map.Entry<String,Map<Integer, List<Integer>>> entry : topicsAndPartitions.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, List<Integer>> partitionMap = entry.getValue();
            Map<Integer, List<Integer>> relevantPartitionsMap = partitionMap.entrySet().stream().filter(m -> m.getValue().contains(brokerId)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            for (Map.Entry<Integer, List<Integer>> entry2 : relevantPartitionsMap.entrySet()) {
                res.add(new Pair<>(topic,entry2.getKey()));
            }
        }
        return res;
    }

    public static Set<KafkaController.PartitionAndReplica> getAllReplicasOnBroker(ZkClient zkClient, List<String> topics,List<Integer> brokerIds) throws JsonProcessingException {
        Set<KafkaController.PartitionAndReplica> res = new HashSet<>();
        for(int brokerId:brokerIds){
            // read all the partitions and their assigned replicas into a map organized by
            // { replica id -> partition 1, partition 2...
            List<Pair<String,Integer>> partitionsAssignedToThisBroker = getPartitionsAssignedToBroker(zkClient, topics, brokerId);
            if(partitionsAssignedToThisBroker.size() == 0)
                logger.info("No state transitions triggered since no partitions are assigned to brokers %s".format(brokerIds.toString()));
            for(Pair<String,Integer> pair:partitionsAssignedToThisBroker){
                res.add(new KafkaController.PartitionAndReplica(pair.getKey(), pair.getValue(), brokerId));
            }
        }
        return res;
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
     * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
     */
    public static Pair<Boolean,Integer> conditionalUpdatePersistentPathIfExists(ZkClient client,String path,String data,int expectVersion){
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            logger.debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
                    .format(path, data, expectVersion, stat.getVersion()));
            return new Pair(true, stat.getVersion());
        } catch(ZkNoNodeException e) {
            throw e;
        }catch(Exception e1) {
            logger.error("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
                    expectVersion, e1.getMessage()));
            return new Pair(false, -1);
        }
    }

    public static List<String> getAllTopics(ZkClient zkClient) {
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if(topics == null)
            return new ArrayList<>();
        else
            return topics;
    }

    public static Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> getPartitionsBeingReassigned(ZkClient zkClient) throws JsonProcessingException {
        // read the partitions and their new replica list
        String jsonPartitionMap = readDataMaybeNull(zkClient, ReassignPartitionsPath).getKey();
        if(jsonPartitionMap == null || jsonPartitionMap.isEmpty()){
            return new HashMap<>();
        }
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> res = new HashMap<>();
        Map<TopicAndPartition, List<Integer>> reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap);
        for (Map.Entry<TopicAndPartition, List<Integer>> entry : reassignedPartitions.entrySet()) {
            res.put(entry.getKey(),new KafkaController.ReassignedPartitionsContext(entry.getValue(),null));
        }
        return res;
    }
    public static Map<TopicAndPartition, List<Integer>> parsePartitionReassignmentData(String jsonData) throws JsonProcessingException {
        Map<TopicAndPartition, List<Integer>> reassignedPartitions = new HashMap<>();
        if(jsonData == null || jsonData.isEmpty()){
            return reassignedPartitions;
        }
        Map<String,Object> map = JacksonUtils.strToMap(jsonData);
        Object obj = map.get("partitions");
        if(obj == null){
            return reassignedPartitions;
        }
        Map<String,Object> p = (Map<String,Object>)obj;
        String topic = p.get("topic").toString();
        int partition = Integer.parseInt(p.get("partition").toString());
        List<Integer> newReplicas = (List<Integer>)p.get("replicas");
        reassignedPartitions .put(new TopicAndPartition(topic, partition) , newReplicas);
        return reassignedPartitions;
    }
    public static  Set<TopicAndPartition> getPartitionsUndergoingPreferredReplicaElection(ZkClient zkClient) throws JsonProcessingException {
        // read the partitions and their new replica list
        String jsonPartitionList = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath).getKey();
        if(jsonPartitionList == null || jsonPartitionList.isEmpty()){
            return new HashSet<>();
        }
        return parsePreferredReplicaElectionData(jsonPartitionList);
    }
    public static  Set<TopicAndPartition> parsePreferredReplicaElectionData(String jsonString) throws JsonProcessingException {
        if(jsonString == null || jsonString.isEmpty()){
            throw new AdministrationException("Preferred replica election data is empty");
        }
        Map<String,Object> map = JacksonUtils.strToMap(jsonString);
        Object obj = map.get("partitions");
        if(obj == null){
            throw new AdministrationException("Preferred replica election data is empty");
        }
        Set<TopicAndPartition> res = new HashSet<>();
        List<Map<String, Object>> partitions = (List<Map<String, Object>>)obj;
        for(Map<String, Object> partitionsMap:partitions){
            String topic = partitionsMap.get("topic").toString();
            int partition = Integer.parseInt(partitionsMap.get("partition").toString());
            res.add(new TopicAndPartition(topic, partition));
        }
        return res;
    }

    public static Map<TopicAndPartition, LeaderIsrAndControllerEpoch> getPartitionLeaderAndIsrForTopics(ZkClient zkClient,  Set<TopicAndPartition> topicAndPartitions) throws IOException {
        Map<TopicAndPartition, LeaderIsrAndControllerEpoch> ret = new HashMap<>();
        for(TopicAndPartition topicAndPartition : topicAndPartitions) {
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic(), topicAndPartition.partition());
            if(leaderIsrAndControllerEpoch != null){
                ret.put(topicAndPartition, leaderIsrAndControllerEpoch);
            }
        }
        return ret;
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
     * exist, the current version is not the expected version, etc.) return (false, -1)
     */
    public static Pair<Boolean,Integer> conditionalUpdatePersistentPath(ZkClient client, String path, String data,int expectVersion) {
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            logger.debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
                    .format(path, data, expectVersion, stat.getVersion()));
            return  new Pair<>(true, stat.getVersion());
        } catch (Exception e){
            logger.error("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
                    expectVersion, e.getMessage()));
            return  new Pair<>(false, -1);
        }
    }
    public static String getPartitionReassignmentZkData(Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
        List<String> jsonPartitionsData = new ArrayList<>();
        for (Map.Entry<TopicAndPartition, List<Integer>> entry : partitionsToBeReassigned.entrySet()) {
            String jsonReplicasData = Utils.seqToJson(entry.getValue().stream().map(x->String.valueOf(x)).collect(Collectors.toList()),  false);
            Map<String, String> m1 = new HashMap<>();
            m1.put("topic",entry.getKey().topic());
            List<String> jsonTopicData = Utils.mapToJsonFields(m1, true);
            Map<String, String> m2 = new HashMap<>();
            m2.put("partition",String.valueOf(entry.getKey().partition()));
            m2.put("replicas",jsonReplicasData);
            List<String> jsonPartitionData = Utils.mapToJsonFields(m2,false);
            jsonTopicData.addAll(jsonPartitionData);
            jsonPartitionsData.add(Utils.mergeJsonFields(jsonTopicData));
        }
        Map<String, String> m3 = new HashMap<>();
        m3.put("version","1");
        m3.put("partitions",Utils.seqToJson(jsonPartitionsData, false));
        return Utils.mapToJson(m3, false);
    }
    public static void updatePartitionReassignmentData(ZkClient zkClient, Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
        String zkPath = ZkUtils.ReassignPartitionsPath;
        if(partitionsToBeReassigned.size() == 0){
            deletePath(zkClient, zkPath);
            logger.info("No more partitions need to be reassigned. Deleting zk path %s".format(zkPath));
        }else{
            String jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned);
            try {
                updatePersistentPath(zkClient, zkPath, jsonData);
                logger.info("Updated partition reassignment path with %s".format(jsonData));
            } catch (ZkNoNodeException nne){
                createPersistentPath(zkClient, zkPath, jsonData);
                logger.debug("Created path %s with %s for partition reassignment".format(zkPath, jsonData));
            }catch (Throwable e2){
                throw new AdministrationException(e2.toString());
            }
        }
    }

    /**
     * Create an persistent node with the given path and data. Create parents if necessary.
     */
    public static void createPersistentPath(ZkClient client, String path, String data) {
        try {
            client.createPersistent(path, data);
        } catch (ZkNoNodeException e){
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
    }

    public static String createSequentialPersistentPath(ZkClient client, String path, String data) {
        return client.createPersistentSequential(path, data);
    }
    public static  class ZKStringSerializer  implements ZkSerializer{
        public byte[] serialize(Object var1) throws ZkMarshallingError{
            try{
                return var1.toString().getBytes("UTF-8");
            }catch (UnsupportedEncodingException e){
                throw  new KafkaException("ZK serialize error:",e);
            }

        }

        public Object deserialize(byte[] bytes) throws ZkMarshallingError{
            try{
                if (bytes == null)
                    return null;
                else
                    return new String(bytes, "UTF-8");
            }catch (UnsupportedEncodingException e){
                throw  new KafkaException("ZK deserialize error:",e);
            }
        }
    }

    public static class ZKGroupDirs{
        String group;
        public ZKGroupDirs(String group){
            this.group =  group;
            consumerDir = ZkUtils.ConsumersPath;
            consumerGroupDir = consumerDir + "/" + group;
            consumerRegistryDir = consumerGroupDir + "/ids";
        }
        public String consumerDir ;
        public String consumerGroupDir ;
        public String consumerRegistryDir ;
    }

    public static  class ZKGroupTopicDirs extends ZKGroupDirs {
        String topic;
        public ZKGroupTopicDirs(String group,String topic){
            super(group);
            this.topic = topic;
            consumerOffsetDir = consumerGroupDir + "/offsets/" + topic;
            consumerOwnerDir = consumerGroupDir + "/owners/" + topic;
        }
        public String consumerOffsetDir ;
        public String consumerOwnerDir ;
    }

    public static  class ZKConfig{

        public  VerifiableProperties props;

        public ZKConfig(VerifiableProperties props){
            this.props = props;
            zkConnect = props.getString("zk.connect");
            zkSessionTimeoutMs = props.getInt( "zk.sessiontimeout.ms", 6000);
            zkConnectionTimeoutMs = props.getInt( "zk.connectiontimeout.ms",zkSessionTimeoutMs);
            zkSyncTimeMs = props.getInt( "zk.synctime.ms", 2000);
        }

        /** ZK host string */
        public String zkConnect;

        /** zookeeper session timeout */
        public int zkSessionTimeoutMs ;

        /** the max time that the client waits to establish a connection to zookeeper */
        public int zkConnectionTimeoutMs ;

        /** how far a ZK follower can be behind a ZK leader */
        public int zkSyncTimeMs ;


    }
}

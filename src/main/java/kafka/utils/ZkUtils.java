package kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.KafkaException;
import kafka.consumer.TopicCount;
import kafka.consumer.TopicCountFactory;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;

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
        readDataMaybeNull(zkClient, ControllerPath)._1 match {
            case Some(controller) => KafkaController.parseControllerId(controller)
            case None => throw new KafkaException("Controller doesn't exist")
        }
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

    public static Broker getAllBrokersInCluster(ZkClient zkClient) {
        List<String> brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.ispublic static Stringined).map(_.get)
    }

    public static LeaderIsrAndControllerEpoch getLeaderIsrAndEpochForPartition(ZkClient zkClient,String topic, int partition){
        val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
        val leaderAndIsrInfo = readDataMaybeNull(zkClient, leaderAndIsrPath)
        val leaderAndIsrOpt = leaderAndIsrInfo._1
        val stat = leaderAndIsrInfo._2
        leaderAndIsrOpt match {
            case Some(leaderAndIsrStr) => parseLeaderAndIsr(leaderAndIsrStr, topic, partition, stat)
            case None => None
        }
    }

    public static LeaderAndIsr getLeaderAndIsrForPartition(ZkClient zkClient,String topic, int partition) {
        getLeaderIsrAndEpochForPartition(zkClient, topic, partition).map(_.leaderAndIsr)
    }

    public static LeaderIsrAndControllerEpoch parseLeaderAndIsr(String leaderAndIsrStr, String topic, int partition,Stat stat) {
        Json.parseFull(leaderAndIsrStr) match {
            case Some(m) =>
                val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
                val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
                val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
                val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
                val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
                val zkPathVersion = stat.getVersion
                debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for partition [%s,%d]".format(leader, epoch,
                        isr.toString(), zkPathVersion, topic, partition))
                Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))
            case None => None
        }
    }

    public static Integer getLeaderForPartition(ZkClient zkClient,String topic, int partition){
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
        leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
                Json.parseFull(leaderAndIsr) match {
                case Some(m) =>
                    Some(m.asInstanceOf[Map[String, Any]].get("leader").get.asInstanceOf[Int])
                case None => None
            }
            case None => None
        }
    }

    /**
     * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
     * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
     * other broker will retry becoming leader with the same new epoch value.
     */
    public static int getEpochForPartition(ZkClient zkClient,String topic, int partition) {
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
        leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
                Json.parseFull(leaderAndIsr) match {
                case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid".format(topic, partition))
                case Some(m) => m.asInstanceOf[Map[String, Any]].get("leader_epoch").get.asInstanceOf[Int]
            }
            case None => throw new NoEpochForPartitionException("No epoch, ISR path for partition [%s,%d] is empty"
                    .format(topic, partition))
        }
    }

    /**
     * Gets the in-sync replicas (ISR) for a specific topic and partition
     */
    public static String getInSyncReplicasForPartition(ZkClient zkClient,String topic, int partition): Seq[Int] = {
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
        leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
                Json.parseFull(leaderAndIsr) match {
                case Some(m) => m.asInstanceOf[Map[String, Any]].get("isr").get.asInstanceOf[Seq[Int]]
                case None => Seq.empty[Int]
            }
            case None => Seq.empty[Int]
        }
    }

    /**
     * Gets the assigned replicas (AR) for a specific topic and partition
     */
    public static List<Integer> getReplicasForPartition(ZkClient zkClient,String topic, int partition) {
        val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
        jsonPartitionMapOpt match {
            case Some(jsonPartitionMap) =>
                Json.parseFull(jsonPartitionMap) match {
                case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
                    case Some(replicaMap) => replicaMap.asInstanceOf[Map[String, Seq[Int]]].get(partition.toString) match {
                        case Some(seq) => seq
                        case None => Seq.empty[Int]
                    }
                    case None => Seq.empty[Int]
                }
                case None => Seq.empty[Int]
            }
            case None => Seq.empty[Int]
        }
    }

    public static boolean isPartitionOnBroker(ZkClient zkClient,String topic, int partition,int  brokerId){
        val replicas = getReplicasForPartition(zkClient, topic, partition);
        debug("The list of replicas for partition [%s,%d] is %s".format(topic, partition, replicas))
        replicas.contains(brokerId.toString)
    }

    public static void registerBrokerInZk(ZkClient zkClient,int id,String host, int port, int timeout, int jmxPort) {
        val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
        val timestamp = "\"" + SystemTime.milliseconds.toString + "\""
        val brokerInfo =
                Utils.mergeJsonFields(Utils.mapToJsonFields(Map("host" -> host), valueInQuotes = true) ++
                Utils.mapToJsonFields(Map("version" -> 1.toString, "jmx_port" -> jmxPort.toString, "port" -> port.toString, "timestamp" -> timestamp),
        valueInQuotes = false))
        val expectedBroker = new Broker(id, host, port)

        try {
            createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
                    (brokerString: String, broker: Any) => Broker.createBroker(broker.asInstanceOf[Broker].id, brokerString).equals(broker.asInstanceOf[Broker]),
                    timeout)

        } catch {
            case e: ZkNodeExistsException =>
                throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                        + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                        + "else you have shutdown this broker and restarted it faster than the zookeeper "
                        + "timeout so it appears to be re-registering.")
        }
        info("Registered broker %d at path %s with address %s:%d.".format(id, brokerIdPath, host, port))
    }

    public static String getConsumerPartitionOwnerPath(String group,String topic,int partition) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
        return topicDirs.consumerOwnerDir + "/" + partition;
    }

    public static String leaderAndIsrZkData(LeaderAndIsr leaderAndIsr, int controllerEpoch) {
        val isrInfo = Utils.seqToJson(leaderAndIsr.isr.map(_.toString), valueInQuotes = false)
        Utils.mapToJson(Map("version" -> 1.toString, "leader" -> leaderAndIsr.leader.toString, "leader_epoch" -> leaderAndIsr.leaderEpoch.toString,
                "controller_epoch" -> controllerEpoch.toString, "isr" -> isrInfo), valueInQuotes = false)
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

    public static String readDataMaybeNull(ZkClient client, String path){
        return client.readData(path, true);
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

    public static Map<String, List<String>> getPartitionsForTopics(ZkClient zkClient, Collection<String> topics){
        Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (String topic : topics) {
            List<String> partList = new ArrayList<String>();
            List<String> brokers = getChildrenParentMayNotExist(zkClient, BrokerTopicsPath + "/" + topic);
            for (String broker : brokers) {
                int nParts = Integer.parseInt(readData(zkClient, BrokerTopicsPath + "/" + topic + "/" + broker));
                for (int part = 0 ; part < nParts;part++)
                    partList.add(broker + "-" + part);
            }
            //  partList = partList.sort((s,t) => s < t);
            ret.put (topic , partList);
        }
        return ret;
    }


    public static void setupPartition(ZkClient zkClient, int brokerId, String host,int port,String topic,int nParts) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        Broker broker = new Broker(brokerId, String.valueOf(brokerId), host, port);
        createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        createEphemeralPathExpectConflict(zkClient, brokerPartTopicPath, String.valueOf(nParts));
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

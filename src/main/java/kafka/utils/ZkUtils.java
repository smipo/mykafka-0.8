package kafka.utils;

import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.consumer.TopicCount;
import kafka.consumer.TopicCountFactory;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

import java.util.*;

public class ZkUtils {

    private static Logger logger = Logger.getLogger(ZkUtils.class);


    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";

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

        public  Properties props;

        public ZKConfig(Properties props){
            this.props = props;
            zkConnect = Utils.getString(props, "zk.connect", null);
            zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000);
            zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms",zkSessionTimeoutMs);
            zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000);
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

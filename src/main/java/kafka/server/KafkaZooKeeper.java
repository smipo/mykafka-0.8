package kafka.server;

import kafka.cluster.Broker;
import kafka.log.LogManager;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;


import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 *
 */
public class KafkaZooKeeper {

    private static Logger logger = Logger.getLogger(KafkaZooKeeper.class);

    KafkaConfig config;
    LogManager logManager;

    public KafkaZooKeeper( KafkaConfig config,
                           LogManager logManager){
        this.config = config;
        this.logManager = logManager;

        brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId;
    }

    String brokerIdPath ;
    ZkClient zkClient = null;
    List<String> topics = new ArrayList<>();
    Object lock = new Object();

    public void startup() {
        /* start client */
        logger.info("connecting to ZK: " + config.zkConnect);
        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,new ZKStringSerializer());
        zkClient.subscribeStateChanges(new SessionExpireListener());
    }

    public void registerBrokerInZk() {
        logger.info("Registering broker " + brokerIdPath);
        String hostName  ;
        if (config.hostName == null){
            try{
                hostName = InetAddress.getLocalHost().getHostAddress();
            }catch (UnknownHostException e){
                hostName = "127.0.0.1";
            }
        }
        else
            hostName = config.hostName;
        String creatorId = hostName + "-" + System.currentTimeMillis();
        Broker broker = new Broker(config.brokerId, creatorId, hostName, config.port);
        try {
            ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        } catch(ZkNodeExistsException e) {
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " +
                    "indicates that you either have configured a brokerid that is already in use, or " +
                    "else you have shutdown this broker and restarted it faster than the zookeeper " +
                    "timeout so it appears to be re-registering.");

        }
        logger.info("Registering broker " + brokerIdPath + " succeeded with " + broker);
    }

    public void registerTopicInZk(String topic) {
        registerTopicInZkInternal(topic);
        synchronized(lock) {
            topics.add(topic);
        }
    }

    public void registerTopicInZkInternal(String topic) {
        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + config.brokerId;
        Integer numParts = logManager.getTopicPartitionsMap().getOrDefault(topic, config.numPartitions);
        logger.info("Begin registering broker topic " + brokerTopicPath + " with " + numParts + " partitions");
        ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, String.valueOf(numParts));
        logger.info("End registering broker topic " + brokerTopicPath);
    }

    public void close() {
        if (zkClient != null) {
            logger.info("Closing zookeeper client...");
            zkClient.close();
        }
    }


    /**
     *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
     *  connection for us. We need to re-register this broker in the broker registry.
     */
    class SessionExpireListener implements IZkStateListener {

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
        public void handleNewSession() throws Exception {
            logger.info("re-registering broker info in ZK for broker " + config.brokerId);
            registerBrokerInZk();
            synchronized(lock) {
                logger.info("re-registering broker topics in ZK for broker " + config.brokerId);
                for (String topic : topics)
                    registerTopicInZkInternal(topic);
            }
            logger.info("done re-registering broker");
        }

        public void handleSessionEstablishmentError(Throwable var1) throws Exception{
            // do nothing,
        }
    }
}

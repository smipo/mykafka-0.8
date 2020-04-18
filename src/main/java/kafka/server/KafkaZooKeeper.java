package kafka.server;

import kafka.cluster.Broker;
import kafka.common.KafkaZookeeperClient;
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

    public KafkaZooKeeper(KafkaConfig config) {
        this.config = config;
        brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId;
    }

    String brokerIdPath ;
    private ZkClient zkClient = null;

    public void startup() throws UnknownHostException {
        /* start client */
        logger.info("connecting to ZK: " + config.zkConnect);
        zkClient = KafkaZookeeperClient.getZookeeperClient(config);
        zkClient.subscribeStateChanges(new SessionExpireListener());
        registerBrokerInZk();
    }

    private void registerBrokerInZk() throws UnknownHostException {
        String hostName ;
        if(config.hostName == null || config.hostName.trim().isEmpty())
            hostName = InetAddress.getLocalHost().getCanonicalHostName();
        else
            hostName = config.hostName;
        int jmxPort = Integer.parseInt(System.getProperty("com.sun.management.jmxremote.port", "-1"));
        ZkUtils.registerBrokerInZk(zkClient, config.brokerId, hostName, config.port, config.zkSessionTimeoutMs, jmxPort);
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
            logger.info("done re-registering broker");
            logger.info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath));
        }
        public void handleSessionEstablishmentError(Throwable var1) throws Exception{

        }
    }

    public void shutdown() {
        if (zkClient != null) {
            logger.info("Closing zookeeper client...");
            zkClient.close();
        }
    }

    public ZkClient getZookeeperClient() {
        return zkClient;
    }
}

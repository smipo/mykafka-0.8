package kafka.consumer;

import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;

import java.util.List;

import static sun.security.jgss.GSSToken.debug;

public class ZookeeperTopicEventWatcher {

    private static Logger logger = Logger.getLogger(ZookeeperTopicEventWatcher.class);

    ConsumerConfig config;
    TopicEventHandler<String> eventHandler;

    public ZookeeperTopicEventWatcher(ConsumerConfig config, TopicEventHandler<String> eventHandler) throws Exception{
        this.config = config;
        this.eventHandler = eventHandler;

        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs,
                config.zkConnectionTimeoutMs, new ZKStringSerializer());
        startWatchingTopicEvents();
    }

    Object lock = new Object();

    private ZkClient zkClient;

    private void startWatchingTopicEvents() throws Exception{
        ZkTopicEventListener topicEventListener = new ZkTopicEventListener();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);

        zkClient.subscribeStateChanges(
                new ZkSessionExpireListener(topicEventListener));

        List<String> topics = zkClient.subscribeChildChanges(
                ZkUtils.BrokerTopicsPath, topicEventListener);

        // call to bootstrap topic list
        topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics);
    }

    private void stopWatchingTopicEvents() {
        zkClient.unsubscribeAll();
    }

    public void shutdown() {
        synchronized(lock) {
            logger.info("Shutting down topic event watcher.");
            if (zkClient != null) {
                stopWatchingTopicEvents();
                zkClient.close();
                zkClient = null;
            } else
                logger.warn("Cannot shutdown already shutdown topic event watcher.");
        }
    }

    class ZkTopicEventListener implements IZkChildListener {


        public  void handleChildChange(String parent, List<String> children) throws Exception{
            synchronized (lock){
                try {
                    if (zkClient != null) {
                        List<String> latestTopics = zkClient.getChildren(ZkUtils.BrokerTopicsPath);
                        logger.debug(String.format("all topics: %s",latestTopics.toString()));
                        eventHandler.handleTopicEvent(latestTopics);
                    }
                } catch (Exception e){
                    logger.error("error in handling child changes", e);
                }
            }
        }
    }

    class ZkSessionExpireListener implements IZkStateListener {


        ZkTopicEventListener topicEventListener;

        public ZkSessionExpireListener(ZkTopicEventListener topicEventListener) {
            this.topicEventListener = topicEventListener;
        }

        public void handleStateChanged(Watcher.Event.KeeperState var1) throws Exception{

        }

        public void handleNewSession() throws Exception {
            synchronized(lock) {
                if (zkClient != null) {
                    logger.info("ZK expired: resubscribing topic event listener to topic registry");
                    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);
                }
            }
        }

        public void handleSessionEstablishmentError(Throwable var1) throws Exception{

        }
    }
}
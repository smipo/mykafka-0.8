package kafka.consumer;

import kafka.api.TopicMetadata;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.TopicAndPartition;
import kafka.server.AbstractFetcherManager;
import kafka.server.AbstractFetcherThread;
import kafka.utils.ShutdownableThread;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ConsumerFetcherManager  extends AbstractFetcherManager {

    private static Logger logger = Logger.getLogger(ConsumerFetcherManager.class);

    String consumerIdString;
    private ConsumerConfig config;
    private ZkClient zkClient ;

    public ConsumerFetcherManager(String consumerIdString, ConsumerConfig config, ZkClient zkClient) {
        super(String.format("ConsumerFetcherManager-%d",System.currentTimeMillis()),
                config.clientId, 1);
        this.consumerIdString = consumerIdString;
        this.config = config;
        this.zkClient = zkClient;
    }

    private Map<TopicAndPartition, PartitionTopicInfo> partitionMap = null;
    private Cluster cluster = null;
    private Set<TopicAndPartition> noLeaderPartitionSet = new HashSet<>();
    private ReentrantLock lock = new ReentrantLock();
    private Condition cond = lock.newCondition();
    private ShutdownableThread leaderFinderThread = null;
    private AtomicInteger correlationId = new AtomicInteger(0);



    private class LeaderFinderThread extends ShutdownableThread {

        public LeaderFinderThread(String name){
            super(name,true);
        }
        // thread responsible for adding the fetcher to the right broker when leader is available
        public  void doWork() throws Throwable{
            Map<TopicAndPartition, Broker> leaderForPartitionsMap = new HashMap<>();
            lock.lock();
            try {
                while (noLeaderPartitionSet.isEmpty()) {
                    logger.trace("No partition for leader election.");
                    cond.await();
                }

                logger.trace(String.format("Partitions without leader %s",noLeaderPartitionSet.toString()));
                List<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
                List<TopicMetadata>  topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.stream().map(m -> m.topic()).collect(Collectors.toSet()),
                        brokers,
                        config.clientId,
                        config.socketTimeoutMs,
                        correlationId.getAndIncrement()).topicsMetadata;
                for(TopicMetadata tmd:topicsMetadata){
                    String topic = tmd.topic;
                    for(TopicMetadata.PartitionMetadata pmd: tmd.partitionsMetadata){
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, pmd.partitionId);
                        if(pmd.leader!=null && noLeaderPartitionSet.contains(topicAndPartition)) {
                            Broker leaderBroker = pmd.leader;
                            leaderForPartitionsMap.put(topicAndPartition, leaderBroker);
                            noLeaderPartitionSet.remove(topicAndPartition);
                        }
                    }
                }
            } catch (Throwable t){
                if (!isRunning.get())
                    throw t; /* If this thread is stopped, propagate this exception to kill the thread. */
                else
                    logger.warn(String.format("Failed to find leader for %s",noLeaderPartitionSet.toString()), t);
            } finally {
                lock.unlock();
            }
            for(Map.Entry<TopicAndPartition, Broker> entry : leaderForPartitionsMap.entrySet()){
                TopicAndPartition topicAndPartition = entry.getKey();
                Broker leaderBroker = entry.getValue();
                PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
                try {
                    addFetcher(topicAndPartition.topic(), topicAndPartition.partition(), pti.getFetchOffset(), leaderBroker);
                } catch (Throwable t){
                    if (!isRunning.get())
                        throw t; /* If this thread is stopped, propagate this exception to kill the thread. */
                    else {
                        logger.warn(String.format("Failed to add leader for partition %s; will retry",topicAndPartition.toString()), t);
                        lock.lock();
                        noLeaderPartitionSet.add(topicAndPartition);
                        lock.unlock();
                    }
                }
            }
            shutdownIdleFetcherThreads();
            Thread.sleep(config.refreshLeaderBackoffMs);
        }
    }

    public AbstractFetcherThread createFetcherThread(int fetcherId, Broker sourceBroker){
        return new ConsumerFetcherThread(
                String.format("ConsumerFetcherThread-%s-%d-%d",consumerIdString, fetcherId, sourceBroker.id()),
                config, sourceBroker, partitionMap, this);
    }

    public void startConnections(Iterable<PartitionTopicInfo> topicInfos,Cluster cluster) {
        leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread");
        leaderFinderThread.start();
        lock.lock();
        try{
            partitionMap = new HashMap<>();
            Iterator<PartitionTopicInfo> iterator = topicInfos.iterator();
            while (iterator.hasNext()){
                PartitionTopicInfo p =  iterator.next();
                partitionMap.put(new TopicAndPartition(p.topic, p.partitionId),p);
                noLeaderPartitionSet.add(new TopicAndPartition(p.topic, p.partitionId));
            }
            this.cluster = cluster;
            cond.signalAll();
        }finally {
            lock.unlock();
        }
    }

    public void stopConnections() throws IOException, InterruptedException {
        /*
         * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
         * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
         * these partitions.
         */
        logger.info("Stopping leader finder thread");
        if (leaderFinderThread != null) {
            leaderFinderThread.shutdown();
            leaderFinderThread = null;
        }

        logger.info("Stopping all fetchers");
        closeAllFetchers();

        // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
        partitionMap = null;
        noLeaderPartitionSet.clear();

        logger.info("All connections stopped");
    }

    public void addPartitionsWithError(Iterable<TopicAndPartition> partitionList) {
        logger.debug(String.format("adding partitions with error %s",partitionList.toString()));
        lock.lock();
        try{
            if (partitionMap != null) {
                while (partitionList.iterator().hasNext()){
                    TopicAndPartition t =  partitionList.iterator().next();
                    noLeaderPartitionSet.add(t);
                }
                cond.signalAll();
            }
        }finally {
            lock.unlock();
        }
    }
}

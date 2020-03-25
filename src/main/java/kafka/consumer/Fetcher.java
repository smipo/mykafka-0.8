package kafka.consumer;

import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Fetcher {

    private static Logger logger = Logger.getLogger(Fetcher.class);

    ConsumerConfig config;
    ZkClient zkClient ;

    public Fetcher(ConsumerConfig config, ZkClient zkClient) {
        this.config = config;
        this.zkClient = zkClient;
    }


    private volatile FetcherRunnable[] fetcherThreads  = null;

    public <T> void clearFetcherQueues(List<BlockingQueue<FetchedDataChunk>> queuesTobeCleared,
                                       Map<String, List<KafkaStream<T>>> messageStreams) {

        // Clear all but the currently iterated upon chunk in the consumer thread's queue
        queuesTobeCleared.stream().forEach(item->item.clear());
        logger.info("Cleared all relevant queues for this fetcher");

        // Also clear the currently iterated upon chunk in the consumer threads
        if(messageStreams != null){
            for (Map.Entry<String, List<KafkaStream<T>>> entry : messageStreams.entrySet()) {
                entry.getValue().stream().forEach(s -> s.clear());
            }
        }
        logger.info("Cleared the data chunks in all the consumer message iterators");
    }

    /**
     *  shutdown all fetcher threads
     */
    public void stopConnectionsToAllBrokers() throws InterruptedException{
        // shutdown the old fetcher threads, if any
        for (FetcherRunnable fetcherThread : fetcherThreads)
            fetcherThread.shutdown();
        fetcherThreads = null;
    }


    public void startConnections(List<PartitionTopicInfo> topicInfos,
                                 Cluster cluster) {
        if (topicInfos == null)
            return;
        // re-arrange by broker id
        Map<Integer,List<PartitionTopicInfo>> m = new HashMap<>();
        // open a new fetcher thread for each broker
        Set<Integer> ids = new HashSet<>();
        for(PartitionTopicInfo info : topicInfos) {
            List<PartitionTopicInfo> list = m.get(info.brokerId);
            if(list == null){
                list = new ArrayList<>();
                m.put(info.brokerId,list);
            }
            list.add(info);
            ids.add(info.brokerId);
        }
        List<Broker> brokers = new ArrayList<>();
        for(Integer id:ids){
            Broker broker = cluster.getBroker(id);
            if(broker == null){
                throw new IllegalStateException("Broker " + id + " is unavailable, fetchers could not be started");
            }
            brokers.add(broker);
        }
        fetcherThreads = new FetcherRunnable[brokers.size()];
        int i = 0;
        for(Broker broker : brokers) {
            FetcherRunnable fetcherThread = new FetcherRunnable("FetchRunnable-" + i, zkClient, config, broker, m.get(broker.id()));
            fetcherThreads[i] = fetcherThread;
            fetcherThread.start();
            i +=1;
        }
    }
}

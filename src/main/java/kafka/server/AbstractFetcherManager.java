package kafka.server;

import kafka.cluster.Broker;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractFetcherManager {

    private static Logger logger = Logger.getLogger(AbstractFetcherManager.class);

    public String name;
    public String metricPrefix;
    public int numFetchers;

    public AbstractFetcherManager(String name, String metricPrefix, int numFetchers) {
        this.name = name;
        this.metricPrefix = metricPrefix;
        this.numFetchers = numFetchers;
    }
    public Map<BrokerAndFetcherId, AbstractFetcherThread> fetcherThreadMap = new HashMap<>();
    public Object mapLock = new Object();

    private int getFetcherId(String topic, int partitionId) {
        return (topic.hashCode() + 31 * partitionId) % numFetchers;
    }

    // to be defined in subclass to create a specific fetcher
   public abstract AbstractFetcherThread createFetcherThread(int fetcherId,Broker sourceBroker);

    public void addFetcher(String topic, int partitionId,long initialOffset,Broker sourceBroker) throws Throwable {
         synchronized(mapLock) {
             AbstractFetcherThread fetcherThread  = null;
             BrokerAndFetcherId key = new BrokerAndFetcherId(sourceBroker, getFetcherId(topic, partitionId));
             AbstractFetcherThread f = fetcherThreadMap.get(key);
             if(f == null){
                 fetcherThread = createFetcherThread(key.fetcherId, sourceBroker);
                 fetcherThreadMap.put(key, fetcherThread);
                 fetcherThread.start();
             }else{
                 fetcherThread = f;
             }

            fetcherThread.addPartition(topic, partitionId, initialOffset);
             logger.info(String
                    .format("Adding fetcher for partition [%s,%d], initOffset %d to broker %d with fetcherId %d",topic, partitionId, initialOffset, sourceBroker.id(), key.fetcherId));
        }
    }

    public void removeFetcher(String topic, int partitionId) throws InterruptedException {
        logger.info(String.format("Removing fetcher for partition [%s,%d]",topic, partitionId));
         synchronized(mapLock) {
             for(Map.Entry<BrokerAndFetcherId, AbstractFetcherThread> entry : fetcherThreadMap.entrySet()){
                 entry.getValue().removePartition(topic, partitionId);
             }
        }
    }

    public void shutdownIdleFetcherThreads() throws InterruptedException, IOException {
         synchronized(mapLock) {
             Set<BrokerAndFetcherId> keysToBeRemoved = new HashSet<>();
             for(Map.Entry<BrokerAndFetcherId, AbstractFetcherThread> entry : fetcherThreadMap.entrySet()){
                 if (entry.getValue().partitionCount() <= 0) {
                     entry.getValue().shutdown();
                     keysToBeRemoved.add(entry.getKey());
                 }
             }
             for(BrokerAndFetcherId id:keysToBeRemoved){
                 fetcherThreadMap.remove(id);
             }
        }
    }

    public void closeAllFetchers() throws IOException, InterruptedException {
         synchronized(mapLock) {
             for(Map.Entry<BrokerAndFetcherId, AbstractFetcherThread> entry : fetcherThreadMap.entrySet()){
                 entry.getValue().shutdown();
            }
            fetcherThreadMap.clear();
        }
    }

    public static class BrokerAndFetcherId{
        public Broker broker;
        public int fetcherId;

        public BrokerAndFetcherId(Broker broker, int fetcherId) {
            this.broker = broker;
            this.fetcherId = fetcherId;
        }
    }
}

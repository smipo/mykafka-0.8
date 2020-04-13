package kafka.producer;


import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.common.UnavailableProducerException;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ProducerPool<V> {

    private static Logger logger = Logger.getLogger(ProducerPool.class);


    /**
     * Used in ProducerPool to initiate a SyncProducer connection with a broker.
     */
    public static SyncProducer createSyncProducer(ProducerConfig config,Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host());
        props.put("port", broker.port()+"");
        props.putAll(config.props.props);
        return new SyncProducer(new SyncProducerConfig(props));
    }

    public ProducerConfig config;

    public ProducerPool(ProducerConfig config) {
        this.config = config;
    }

    public ConcurrentMap<Integer, SyncProducer> syncProducers = new ConcurrentHashMap<>();
    private Object lock = new Object();

    public void updateProducer(List<TopicMetadata> topicMetadata) {
        Set<Broker> newBrokers = new HashSet<>();
        for(TopicMetadata tmd:topicMetadata){
            for(TopicMetadata.PartitionMetadata pmd:tmd.partitionsMetadata){
                if(pmd.leader != null)
                    newBrokers.add(pmd.leader);
            }
        }
         synchronized(lock) {
            for(Broker b:newBrokers){
                if(syncProducers.containsKey(b.id())){
                    syncProducers.get(b.id()).close();
                    syncProducers.put(b.id(), ProducerPool.createSyncProducer(config, b));
                } else
                    syncProducers.put(b.id(), ProducerPool.createSyncProducer(config, b));
            }
        }
    }

    public SyncProducer getProducer(int brokerId) {
        synchronized (lock){
            SyncProducer producer = syncProducers.get(brokerId);
            if(producer == null){
                throw new UnavailableProducerException("Sync producer for broker id %d does not exist".format(brokerId+""));
            }
            return producer;
        }
    }

    /**
     * Closes all the producers in the pool
     */
    public void close() {
        synchronized(lock) {
            logger.info("Closing all sync producers");
            Iterator<SyncProducer> iter = syncProducers.values().iterator();
            while(iter.hasNext())
                iter.next().close();
        }
    }

}

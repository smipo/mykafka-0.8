package kafka.server;

import kafka.cluster.Broker;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ReplicaFetcherManager  extends AbstractFetcherManager{

    private static Logger logger = Logger.getLogger(ReplicaFetcherManager.class);

    KafkaConfig brokerConfig ;
    ReplicaManager replicaMgr;

    public ReplicaFetcherManager(KafkaConfig brokerConfig, ReplicaManager replicaMgr) {
        super("ReplicaFetcherManager on broker " + brokerConfig.brokerId,
                "Replica", brokerConfig.numReplicaFetchers);
        this.brokerConfig = brokerConfig;
        this.replicaMgr = replicaMgr;
    }

    public AbstractFetcherThread createFetcherThread(int fetcherId, Broker sourceBroker){
       return new ReplicaFetcherThread(String.format("ReplicaFetcherThread-%d-%d",fetcherId, sourceBroker.id()), sourceBroker, brokerConfig, replicaMgr);
    }

    public void shutdown() throws IOException, InterruptedException {
        logger.info("shutting down");
        closeAllFetchers();
        logger.info("shutdown completed");
    }
}

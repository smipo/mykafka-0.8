package kafka.producer;

import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.common.InvalidConfigException;
import kafka.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class ConfigBrokerPartitionInfo implements BrokerPartitionInfo {

    ProducerConfig config;

    public ConfigBrokerPartitionInfo(ProducerConfig config) {
        this.config = config;

        brokerPartitions = getConfigTopicPartitionInfo();
        allBrokers = getConfigBrokerInfo();
    }

    private SortedSet<Partition> brokerPartitions;
    private Map<Integer, Broker> allBrokers ;

    /**
     * Return a sequence of (brokerId, numPartitions)
     * @param topic this value is null
     * @return a sequence of (brokerId, numPartitions)
     */
    public SortedSet<Partition> getBrokerPartitionInfo(String topic){
        return brokerPartitions;
    }

    /**
     * Generate the host and port information for the broker identified
     * by the given broker id
     * @param brokerId the broker for which the info is to be returned
     * @return host and port of brokerId
     */
    public Broker getBrokerInfo(int brokerId) {
        return allBrokers.get(brokerId);
    }

    /**
     * Generate a mapping from broker id to the host and port for all brokers
     * @return mapping from id to host and port of all brokers
     */
    public Map<Integer, Broker> getAllBrokerInfo(){
        return allBrokers;
    }

    public void close() {}

    public void  updateInfo(){}

    /**
     * Generate a sequence of (brokerId, numPartitions) for all brokers
     * specified in the producer configuration
     * @return sequence of (brokerId, numPartitions)
     */
    private SortedSet<Partition> getConfigTopicPartitionInfo(){
        String[] brokerInfoList = config.brokerList.split(",");
        if(brokerInfoList.length == 0) throw new InvalidConfigException("broker.list is empty");
        // check if each individual broker info is valid => (brokerId: brokerHost: brokerPort)
        SortedSet<Partition> brokerParts = Utils.getTreeSetSet();
        for(String bInfo:brokerInfoList){
            String[] brokerInfo = bInfo.split(":");
            if(brokerInfo.length < 3) throw new InvalidConfigException("broker.list has invalid value");
            String head = bInfo.split(":")[0];
            Partition bidPid = new Partition(Integer.parseInt(head), 0);
            brokerParts.add(bidPid);
        }
        return brokerParts;
    }

    /**
     * Generate the host and port information for for all brokers
     * specified in the producer configuration
     * @return mapping from brokerId to (host, port) for all brokers
     */
    private Map<Integer, Broker> getConfigBrokerInfo() {
        Map<Integer, Broker> brokerInfo = new HashMap<>();
        String[] brokerInfoList = config.brokerList.split(",");
        for(String bInfo:brokerInfoList){
            String[] brokerIdHostPort = bInfo.split(":");
            brokerInfo.put(Integer.parseInt(brokerIdHostPort[0]) , new Broker(Integer.parseInt(brokerIdHostPort[0]), brokerIdHostPort[1],
                    brokerIdHostPort[1], Integer.parseInt(brokerIdHostPort[2])));
        }
        return brokerInfo;
    }
}

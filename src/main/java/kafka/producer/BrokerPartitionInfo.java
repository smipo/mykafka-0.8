package kafka.producer;

import kafka.cluster.Broker;
import kafka.cluster.Partition;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public interface BrokerPartitionInfo {

    /**
     * Return a sequence of (brokerId, numPartitions).
     * @param topic the topic for which this information is to be returned
     * @return a sequence of (brokerId, numPartitions). Returns a zero-length
     * sequence if no brokers are available.
     */
    public SortedSet<Partition> getBrokerPartitionInfo(String topic);

    /**
     * Generate the host and port information for the broker identified
     * by the given broker id
     * @param brokerId the broker for which the info is to be returned
     * @return host and port of brokerId
     */
    public Broker getBrokerInfo(int brokerId);

    /**
     * Generate a mapping from broker id to the host and port for all brokers
     * @return mapping from id to host and port of all brokers
     */
    public Map<Integer, Broker> getAllBrokerInfo();

    /**
     * This is relevant to the ZKBrokerPartitionInfo. It updates the ZK cache
     * by reading from zookeeper and recreating the data structures. This API
     * is invoked by the producer, when it detects that the ZK cache of
     * ZKBrokerPartitionInfo is stale.
     *
     */
    public void updateInfo();

    /**
     * Cleanup
     */
    public void close();
}

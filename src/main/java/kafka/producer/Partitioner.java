package kafka.producer;

public interface Partitioner<T> {

    /**
     * Uses the key to calculate a partition bucket id for routing
     * the data to the appropriate broker partition
     * @return an integer between 0 and numPartitions-1
     */
    public int partition(T key, int numPartitions);
}

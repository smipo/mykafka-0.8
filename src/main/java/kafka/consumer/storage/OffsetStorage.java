package kafka.consumer.storage;
/**
 * A method for storing offsets for the consumer.
 * This is used to track the progress of the consumer in the stream.
 */
public interface OffsetStorage {

    /**
     * Reserve a range of the length given by increment.
     * @param increment The size to reserver
     * @return The range reserved
     */
    long reserve(int node, String topic);

    /**
     * Update the offset to the new offset
     * @param offset The new offset
     */
    void commit(int node, String topic, long offset);
}

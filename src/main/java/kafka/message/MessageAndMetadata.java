package kafka.message;

public class MessageAndMetadata<K,V> {

    K key;
    V message;
    String topic;
    int partition;
    long offset;

    public MessageAndMetadata(K key, V message, String topic, int partition, long offset) {
        this.key = key;
        this.message = message;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public K key() {
        return key;
    }

    public MessageAndMetadata key(K key) {
        this.key = key;
        return this;
    }

    public V message() {
        return message;
    }

    public MessageAndMetadata message(V message) {
        this.message = message;
        return this;
    }

    public String topic() {
        return topic;
    }

    public MessageAndMetadata topic(String topic) {
        this.topic = topic;
        return this;
    }

    public int partition() {
        return partition;
    }

    public MessageAndMetadata partition(int partition) {
        this.partition = partition;
        return this;
    }

    public long offset() {
        return offset;
    }

    public MessageAndMetadata offset(long offset) {
        this.offset = offset;
        return this;
    }
}

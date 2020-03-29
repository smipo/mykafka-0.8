package kafka.common;

public class TopicAndPartition {

    String topic;
    int partition;

    public TopicAndPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }
}

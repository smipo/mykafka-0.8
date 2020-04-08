package kafka.common;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicAndPartition that = (TopicAndPartition) o;
        return partition == that.partition &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return "TopicAndPartition{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}

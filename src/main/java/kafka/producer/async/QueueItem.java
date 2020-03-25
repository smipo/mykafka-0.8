package kafka.producer.async;

import java.util.Objects;

public class QueueItem<T> {

    T data;
    String topic;
    int partition;

    public QueueItem(T data, String topic, int partition) {
        this.data = data;
        this.topic = topic;
        this.partition = partition;
    }

    public T getData() {
        return data;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String toString (){
        return "topic: " + topic + ", partition: " + partition + ", data: " + data.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueItem<?> queueItem = (QueueItem<?>) o;
        return partition == queueItem.partition &&
                Objects.equals(data, queueItem.data) &&
                Objects.equals(topic, queueItem.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, topic, partition);
    }
}

package kafka.javaapi;

import kafka.api.RequestKeys;
import kafka.javaapi.message.ByteBufferMessageSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ProducerRequest extends Request {

    String topic;
    int partition;
    ByteBufferMessageSet messages;

    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        super(RequestKeys.Produce);
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;

        underlying = new kafka.api.ProducerRequest(topic, partition, messages.underlying());
    }
    kafka.api.ProducerRequest underlying ;

    public void writeTo(ByteBuffer buffer) throws IOException { underlying.writeTo(buffer); }

    public  int sizeInBytes(){
        return underlying.sizeInBytes();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerRequest that = (ProducerRequest) o;
        return partition == that.partition &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(messages, that.messages);
    }

    public int hashCode(){
        return 31 + (17 * partition) + topic.hashCode() + messages.hashCode();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public ByteBufferMessageSet messages() {
        return messages;
    }

    public kafka.api.ProducerRequest underlying() {
        return underlying;
    }
}

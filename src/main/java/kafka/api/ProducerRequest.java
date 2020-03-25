package kafka.api;

import kafka.message.ByteBufferMessageSet;
import kafka.network.Request;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;


public class ProducerRequest extends Request {

    public static int RandomPartition = -1;

    String topic;
    int partition;
    ByteBufferMessageSet messages;

    public ProducerRequest( String topic,
                            int partition,
                            ByteBufferMessageSet messages){
        super(RequestKeys.Produce);
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    public static ProducerRequest readFrom(ByteBuffer buffer) throws IOException{
        String topic = Utils.readShortString(buffer, "UTF-8");
        int partition = buffer.getInt();
        int messageSetSize = buffer.getInt();
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new ProducerRequest(topic, partition, new ByteBufferMessageSet(messageSetBuffer));
    }
    public void writeTo(ByteBuffer buffer) throws IOException {
        Utils.writeShortString(buffer, topic, "UTF-8");
        buffer.putInt(partition);
        buffer.putInt(messages.serialized().limit());
        buffer.put(messages.serialized());
        messages.serialized().rewind();
    }

    public int sizeInBytes(){
        return 2 + topic.length() + 4 + 4 + (int)messages.sizeInBytes();
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

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append("ProducerRequest(");
        builder.append(topic + ",");
        builder.append(partition + ",");
        builder.append(messages.sizeInBytes());
        builder.append(")");
        return builder.toString();
    }

    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj instanceof ProducerRequest){
            ProducerRequest that = (ProducerRequest)obj;
            return topic == that.topic && partition == that.partition &&
                    messages.equals(that.messages);
        }
        return false;
    }

    public int hashCode(){
        return 31 + (17 * partition) + topic.hashCode() + messages.hashCode();
    }

}

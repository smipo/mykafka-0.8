package kafka.api;

import kafka.network.Request;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FetchRequest extends Request {

    String topic;
    int partition;
    long offset;
    int maxSize;

    public FetchRequest( String topic,
            int partition,
            long offset,
            int maxSize){
        super(RequestKeys.Fetch);
        this.topic= topic;
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

    public static FetchRequest readFrom(ByteBuffer buffer) throws IOException{
        String topic = Utils.readShortString(buffer, "UTF-8");
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }

   public void writeTo(ByteBuffer buffer) throws IOException {
        Utils.writeShortString(buffer, topic, "UTF-8");
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
    }

    public int sizeInBytes(){
        return  2 + topic.length() + 4 + 8 + 4;
    }

    @Override
    public String toString(){
        return "FetchRequest(topic:" + topic + ", part:" + partition +" offset:" + offset +
                " maxSize:" + maxSize + ")";
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public int maxSize() {
        return maxSize;
    }
}

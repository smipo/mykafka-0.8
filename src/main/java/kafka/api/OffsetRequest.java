package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.Request;
import kafka.network.Send;
import kafka.utils.Utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class OffsetRequest  extends Request {

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;


    public static OffsetRequest readFrom(ByteBuffer buffer) throws UnsupportedEncodingException {
        String topic = Utils.readShortString(buffer, "UTF-8");
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxNumOffsets = buffer.getInt();
        return new OffsetRequest(topic, partition, offset, maxNumOffsets);
    }

    public static ByteBuffer serializeOffsetArray(long[] offsets){
        int size = 4 + 8 * offsets.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(offsets.length);
        for (int i = 0;i < offsets.length;i++)
            buffer.putLong(offsets[i]);
        buffer.rewind();
        return  buffer;
    }

    public static long[] deserializeOffsetArray(ByteBuffer buffer){
        int size = buffer.getInt();
        long[] offsets = new long[size];
        for (int i = 0;i < offsets.length;i++)
            offsets[i] = buffer.getLong();
        return offsets;
    }

    public String topic;
    public int partition;
    public long time;
    public int maxNumOffsets;

    public OffsetRequest(  String topic,
            int partition,
            long time,
            int maxNumOffsets){
        super(RequestKeys.Offsets);
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    public void writeTo(ByteBuffer buffer)throws UnsupportedEncodingException {
        Utils.writeShortString(buffer, topic, "UTF-8");
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    public int sizeInBytes(){
       return 2 + topic.length() + 4 + 8 + 4;
    }

    public String toString(){
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time +
                ", maxNumOffsets:" + maxNumOffsets + ")";
    }


    public static class OffsetArraySend extends Send{

        long[] offsets;

        private int size;
        private ByteBuffer header = ByteBuffer.allocate(6);
        private volatile boolean complete;
        private ByteBuffer contentBuffer;

        public OffsetArraySend(long[] offsets){
            this.offsets = offsets;
            this.contentBuffer = OffsetRequest.serializeOffsetArray(offsets);
            //todo
            int sum = 4;
            for(long offset:offsets ){
                sum += 8;
            }
            this.size = sum;
            this.header.putInt(size + 2);
            this.header.putShort((short) ErrorMapping.NoError);
            this.header.rewind();
        }

        public  long writeTo(GatheringByteChannel channel) throws IOException{
            expectIncomplete();
            int written = 0;
            if(header.hasRemaining())
                written += channel.write(header);
            if(!header.hasRemaining() && contentBuffer.hasRemaining())
                written += channel.write(contentBuffer);

            if(!contentBuffer.hasRemaining())
                complete = true;
            return written;
        }

        public  boolean complete(){
            return complete;
        }
    }
}

package kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class ByteBufferSend  extends Send {

    ByteBuffer buffer;

   volatile boolean complete;

    public ByteBufferSend(ByteBuffer buffer){
        this.buffer = buffer;
    }

    public ByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public long writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        written += channel.write(buffer);
        if(!buffer.hasRemaining())
            complete = true;
        return written;
    }

    public  boolean complete(){
        return complete;
    }

    public ByteBuffer buffer() {
        return buffer;
    }
}

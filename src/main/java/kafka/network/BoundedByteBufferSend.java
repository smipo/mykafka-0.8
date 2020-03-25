package kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class BoundedByteBufferSend extends Send {

    private ByteBuffer buffer;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    private volatile boolean complete = false;

    public BoundedByteBufferSend(Request request) throws IOException  {
        this(request.sizeInBytes() + 2);
        buffer.putShort(request.id());
        request.writeTo(buffer);
        buffer.rewind();
    }
    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }
    public BoundedByteBufferSend(ByteBuffer buffer){
        this.buffer = buffer;
        if(buffer.remaining() > Integer.MAX_VALUE  - sizeBuffer.limit())
            throw new IllegalArgumentException("Attempt to create a bounded buffer of " + buffer.remaining() + " bytes, but the maximum " +
                    "allowable size for a bounded buffer is " + (Integer.MAX_VALUE - sizeBuffer.limit()) + ".");
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }

    // Avoid possibility of overflow for 2GB-4 byte buffer


    @Override
    public boolean complete(){
        return complete;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        ByteBuffer[] buffers = new ByteBuffer[2];
        buffers[0] = sizeBuffer;
        buffers[1] = buffer;
        long written = channel.write(buffers);
        // if we are done, mark it off
        if(!buffer.hasRemaining())
            complete = true;
        return written;
    }

    public ByteBuffer buffer() {
        return buffer;
    }
}

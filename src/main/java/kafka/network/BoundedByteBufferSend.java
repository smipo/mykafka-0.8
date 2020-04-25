package kafka.network;

import kafka.api.RequestOrResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class BoundedByteBufferSend extends Send {

    public ByteBuffer buffer;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    private volatile boolean complete = false;

    public BoundedByteBufferSend(RequestOrResponse request) throws IOException  {
        this(request.sizeInBytes() + (request.requestId != null ? 2 : 0));
        if(request.requestId != null){
            buffer.putShort(request.requestId);
        }
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
        ByteBuffer[] srcs = new ByteBuffer[2];
        srcs[0] = sizeBuffer;
        srcs[1] = buffer;
        long written = channel.write(srcs);
        // if we are done, mark it off
        if(!buffer.hasRemaining())
            complete = true;
        return written;
    }

    public ByteBuffer buffer() {
        return buffer;
    }
}

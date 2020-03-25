package kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class BoundedByteBufferReceive extends Receive {

    private int maxSize;

    private boolean complete = false;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer contentBuffer = null;

    public  BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }
    public  BoundedByteBufferReceive(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean complete(){
        return complete;
    }

    /**
     * Get the content buffer for this transmission
     */
    @Override
    public ByteBuffer buffer() {
        expectComplete();
        return contentBuffer;
    }

    /**
     * Read the bytes in this response from the given channel
     */
    @Override
    public long readFrom(ReadableByteChannel channel) throws IOException {
        expectIncomplete();
        long read = 0;
        // have we read the request size yet?
        if(sizeBuffer.remaining() > 0)
            read += channel.read(sizeBuffer);
        // have we allocated the request buffer yet?
        if(contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind();
            int size = sizeBuffer.getInt();
            if(size <= 0)
                throw new InvalidRequestException("%d is not a valid request size.".format(String.valueOf(size)));
            if(size > maxSize)
                throw new InvalidRequestException("Request of length %d is not valid, it is larger than the maximum size of %d bytes.".format(String.valueOf(size), maxSize));
            contentBuffer = byteBufferAllocate(size);
        }
        // if we have a buffer read some stuff into it
        if(contentBuffer != null) {
            read = channel.read(contentBuffer);
            // did we get everything?
            if(!contentBuffer.hasRemaining()) {
                contentBuffer.rewind();
                complete = true;
            }
        }
       return read;
    }

    private ByteBuffer byteBufferAllocate(int size)  {
        return ByteBuffer.allocate(size);
    }
}

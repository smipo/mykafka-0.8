package kafka.message;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {

    private ByteBuffer buffer;

    public ByteBufferBackedInputStream(ByteBuffer buffer){
        this.buffer = buffer;
    }
    @Override
    public int read(){
        if(buffer.hasRemaining()){
            return buffer.get() & 0xFF;
        }
        return -1;
    }
    @Override
    public int read(byte[] bytes, int off, int len) {
        if(buffer.hasRemaining()){
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        }
        return -1;
    }
}

package kafka.javaapi.message;

import kafka.common.ErrorMapping;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.NoCompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;


public class ByteBufferMessageSet extends MessageSet {

    ByteBuffer buffer;
    private long initialOffset = 0L;
    private int errorCode = ErrorMapping.NoError;

    public ByteBufferMessageSet(ByteBuffer buffer, long initialOffset, int errorCode) {
      init(buffer,initialOffset,errorCode);
    }

    public ByteBufferMessageSet(ByteBuffer buffer) {
        init(buffer, 0L, ErrorMapping.NoError);
    }

    public ByteBufferMessageSet(CompressionCodec compressionCodec,java.util.List<Message> messages) throws IOException {
        Message[] messages1 = new Message[messages.size()];
        init(kafka.message.MessageSet.createByteBuffer(compressionCodec,messages.toArray(messages1)),
                0L, ErrorMapping.NoError);
    }

    private void init(ByteBuffer buffer, long initialOffset, int errorCode){
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;

        underlying = new kafka.message.ByteBufferMessageSet(buffer,
                initialOffset,
                errorCode);
    }
    public ByteBufferMessageSet(java.util.List<Message> messages) throws IOException{
        this(new NoCompressionCodec(), messages);
    }

    kafka.message.ByteBufferMessageSet underlying;

    public long validBytes(){
        return underlying.validBytes();
    }

    public ByteBuffer serialized() {
        return underlying.serialized();
    }

    public long getInitialOffset(){
        return initialOffset;
    }

    public ByteBuffer getBuffer(){
        return buffer;
    }

    public int getErrorCode (){
        return errorCode;
    }

    public  Iterator<MessageAndOffset> iterator() {
        return new Iterator<MessageAndOffset>() {
            Iterator<MessageAndOffset> underlyingIterator = underlying.iterator();

            public boolean hasNext() {
                return underlyingIterator.hasNext();
            }

            public MessageAndOffset next() {
                return underlyingIterator.next();
            }

            public void remove() {
                throw new UnsupportedOperationException("remove API on MessageSet is not supported");
            }
        };
    }
    public String toString(){
        return underlying.toString();
    }

    public long sizeInBytes(){
        return underlying.sizeInBytes();
    }

    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj instanceof ByteBufferMessageSet){
            ByteBufferMessageSet that = (ByteBufferMessageSet)obj;
            return errorCode == that.errorCode && buffer.equals(that.buffer) && initialOffset == that.initialOffset;
        }
        return false;
    }

    public int hashCode(){
        return 31 * (17 + errorCode) + buffer.hashCode() + new Long(initialOffset).hashCode();
    }

    public kafka.message.ByteBufferMessageSet underlying() {
        return underlying;
    }
}

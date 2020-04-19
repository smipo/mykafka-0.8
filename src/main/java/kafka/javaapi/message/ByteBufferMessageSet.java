package kafka.javaapi.message;

import kafka.common.ErrorMapping;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.NoCompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class ByteBufferMessageSet extends MessageSet {

    public static ByteBuffer getByteBuffer(CompressionCodec compressionCodec, List<Message> messageList) throws IOException {
        Message[] messages = new Message[messageList.size()];
        kafka.message.ByteBufferMessageSet bufferMessageSet = new kafka.message.ByteBufferMessageSet(compressionCodec, new AtomicLong(0), messageList.toArray(messages));
        return bufferMessageSet.buffer();
    }

    ByteBuffer buffer;

    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
        underlying = new kafka.message.ByteBufferMessageSet(buffer);
    }

    kafka.message.ByteBufferMessageSet underlying;


    public ByteBufferMessageSet(CompressionCodec compressionCodec, List<Message> messageList) throws IOException {
        // due to SI-4141 which affects Scala 2.8.1, implicits are not visible in constructors and must be used explicitly
        this(getByteBuffer(compressionCodec,messageList));
    }

    public ByteBufferMessageSet(List<Message> messages) throws IOException {
        this(new NoCompressionCodec(), messages);
    }

    public long validBytes(){
        return underlying.validBytes();
    }

    public  Iterator<MessageAndOffset> iterator() {
        Iterator<MessageAndOffset> underlyingIterator = underlying.iterator();
        return underlyingIterator;
    }

    public long sizeInBytes() {
        return underlying.sizeInBytes();
    }


}

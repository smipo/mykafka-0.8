package kafka.message;

import kafka.common.ErrorMapping;
import kafka.common.InvalidMessageSizeException;
import kafka.common.MessageSizeTooLargeException;
import kafka.utils.IteratorTemplate;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 */
public class ByteBufferMessageSet extends MessageSet {

    private static Logger logger = Logger.getLogger(ByteBufferMessageSet.class);


    private static ByteBuffer create(AtomicLong offsetCounter, CompressionCodec compressionCodec, Message ...messages) throws IOException{
        if(messages.length == 0) {
            return MessageSet.Empty.buffer();
        } else if(compressionCodec instanceof  NoCompressionCodec) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
            for(Message message : messages)
                writeMessage(buffer, message, offsetCounter.getAndIncrement());
            buffer.rewind();
            return buffer;
        } else {
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages));
            DataOutputStream output = new DataOutputStream(CompressionFactory.getOutputStream(compressionCodec, byteArrayStream));
            long offset = -1L;
            try {
                for(Message message : messages) {
                    offset = offsetCounter.getAndIncrement();
                    output.writeLong(offset);
                    output.writeInt(message.size());
                    output.write(message.buffer.array(), message.buffer.arrayOffset(), message.buffer.limit());
                }
            } finally {
                output.close();
            }
            byte[] bytes = byteArrayStream.toByteArray();
            Message message = new Message(bytes, compressionCodec);
            ByteBuffer buffer = ByteBuffer.allocate(message.size() + MessageSet.LogOverhead);
            writeMessage(buffer, message, offset);
            buffer.rewind();
            return  buffer;
        }
    }

    public static ByteBufferMessageSet decompress(Message message) throws IOException{
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        byte[] intermediateBuffer = new byte[1024];
        InputStream compressed = CompressionFactory.getInputStream(message.compressionCodec(), inputStream);
        try {
            int len;
            while ((len = compressed.read(intermediateBuffer)) != -1){
                outputStream.write(intermediateBuffer, 0, len);
            }
        } finally {
            compressed.close();
        }
        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        return new ByteBufferMessageSet(outputBuffer);
    }

    private static void writeMessage(ByteBuffer buffer, Message message, long offset) {
        buffer.putLong(offset);
        buffer.putInt(message.size());
        buffer.put(message.buffer);
        message.buffer.rewind();
    }

    private ByteBuffer buffer;

    private int shallowValidByteCount = -1;


    public ByteBufferMessageSet(CompressionCodec compressionCodec, Message...messages) throws IOException{
        this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, messages));
    }

    public ByteBufferMessageSet(CompressionCodec compressionCodec, AtomicLong offsetCounter, Message...messages)throws IOException {
        this(ByteBufferMessageSet.create(offsetCounter, compressionCodec, messages));
    }

    public ByteBufferMessageSet(Message...messages)throws IOException {
        this(new NoCompressionCodec(), new AtomicLong(0), messages);
    }

    public ByteBufferMessageSet(ByteBuffer buffer){
        this.buffer = buffer;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public long validBytes(){
        return shallowValidBytes();
    }

    private int shallowValidBytes(){
        if(shallowValidByteCount < 0) {
            int bytes = 0;
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while(iter.hasNext()) {
                MessageAndOffset messageAndOffset = iter.next();
                bytes += MessageSet.entrySize(messageAndOffset.message());
            }
            this.shallowValidByteCount = bytes;
        }
      return shallowValidByteCount;
    }

    /** Write the messages in this set to the given channel */
    public long writeTo(GatheringByteChannel channel, long offset, int size) throws IOException {
        buffer.mark();
        long written = channel.write(buffer);
        buffer.reset();
        return written;
    }

    /** default iterator that iterates over decompressed messages */
    public Iterator<MessageAndOffset> iterator(){
        return internalIterator(false);
    }

    /** iterator over compressed messages without decompressing */
    public Iterator<MessageAndOffset> shallowIterator(){
        return internalIterator(true);
    }

    public void verifyMessageSize(int maxMessageSize){
        Iterator<MessageAndOffset> shallowIter = internalIterator(true);
        while(shallowIter.hasNext()){
            MessageAndOffset messageAndOffset = shallowIter.next();
            int payloadSize = messageAndOffset.message().payloadSize();
            if ( payloadSize > maxMessageSize)
                throw new MessageSizeTooLargeException("payload size of " + payloadSize + " larger than " + maxMessageSize);
        }
    }

    /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. This is used in verifyMessageSize() function **/
    private Iterator<MessageAndOffset> internalIterator(final boolean isShallow) {

       return new IteratorTemplate<MessageAndOffset>(){
            ByteBuffer topIter = buffer.slice();
            Iterator<MessageAndOffset> innerIter = null;

            public boolean innerDone(){
                return (innerIter==null || !innerIter.hasNext());
            }

            public MessageAndOffset makeNextOuter() {
                if (topIter.remaining() < 12)
                    return allDone();
                long offset = topIter.getLong();
                int size = topIter.getInt();

                if(size < Message.MinHeaderSize)
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")");

                // we have an incomplete message
                if(topIter.remaining() < size)
                    return allDone();

                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                Message newMessage = new Message(message);

                if(isShallow){
                    return new MessageAndOffset(newMessage, offset);
                }
                else{
                    if(newMessage.compressionCodec() instanceof NoCompressionCodec){
                         innerIter = null;
                        return new MessageAndOffset(newMessage, offset);
                    }else{
                        try{
                            innerIter = ByteBufferMessageSet.decompress(newMessage).internalIterator(false);
                        }catch (IOException e) {
                            logger.error("ByteBufferMessageSet iterator error:",e);
                        }
                        if(!innerIter.hasNext())
                            innerIter = null;
                       return makeNext();
                    }
                }
            }

            public MessageAndOffset makeNext() {
                if(isShallow){
                   return makeNextOuter();
                } else {
                    if(innerDone())
                        return  makeNextOuter();
                    else
                        return  innerIter.next();
                }
            }
        };
    }

    /**
     * Update the offsets for this message set. This method attempts to do an in-place conversion
     * if there is no compression, but otherwise recopies the messages
     */
    public ByteBufferMessageSet assignOffsets(AtomicLong offsetCounter, CompressionCodec codec) throws IOException{
        if(codec instanceof NoCompressionCodec) {
            // do an in-place conversion
            int position = 0;
            buffer.mark();
            while(position < sizeInBytes() - MessageSet.LogOverhead) {
                buffer.position(position);
                buffer.putLong(offsetCounter.getAndIncrement());
                position += MessageSet.LogOverhead + buffer.getInt();
            }
            buffer.reset();
            return this;
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            List<Message> list = new ArrayList<>();
            Iterator<MessageAndOffset> iterator = this.internalIterator(false);
            while (iterator.hasNext()){
                MessageAndOffset messageAndOffset = iterator.next();
                list.add(messageAndOffset.message());
            }
            Message[] messages = new Message[list.size()];
            return new ByteBufferMessageSet(codec, offsetCounter, list.toArray(messages));
        }
    }

    public int sizeInBytes(){
        return buffer.limit();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteBufferMessageSet that = (ByteBufferMessageSet) o;
        return Objects.equals(buffer, that.buffer);
    }

    @Override
    public int hashCode(){
        return buffer.hashCode();
    }

}

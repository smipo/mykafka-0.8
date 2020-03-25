package kafka.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {

    public static int LogOverhead = 4;


    /**
     * The size of a list of messages
     */
    public static int messageSetSize(Message...messages){
        int size = 0;
        for(Message message:messages){
            size += entrySize(message);
        }
        return size;
    }

    /**
     * The size of a size-delimited entry in a message set
     */
    public static int entrySize(Message message){
        return LogOverhead + message.size();
    }

    public static ByteBuffer createByteBuffer(CompressionCodec compressionCodec, Message...messages)throws IOException {
        if (compressionCodec instanceof NoCompressionCodec) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
            for (Message message : messages) {
                message.serializeTo(buffer);
            }
            buffer.rewind();
            return buffer;
        } else {
            if(messages.length == 0){
                ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
                buffer.rewind();
                return buffer;
            }
            Message message = CompressionFactory.compress(compressionCodec,messages);
            ByteBuffer buffer = ByteBuffer.allocate(message.serializedSize());
            message.serializeTo(buffer);
            buffer.rewind();
            return buffer;
        }
    }

    /** Write the messages in this set to the given channel starting at the given offset byte.
     * Less than the complete amount may be written, but no more than maxSize can be. The number
     * of bytes written is returned */
    public abstract long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException;

    /**
     * Provides an iterator over the messages in this set
     */
    public abstract Iterator<MessageAndOffset> iterator();

    /**
     * Gives the total size of this message set in bytes
     */
    public abstract long sizeInBytes();

    /**
     * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
     * match the payload for any message.
     */
    public void validate() {
        for(MessageAndOffset messageAndOffset : this){
            if(!messageAndOffset.message().isValid())
                throw new InvalidMessageException();
        }
    }
}

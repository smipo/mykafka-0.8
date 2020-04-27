package kafka.log;

import kafka.common.InvalidIOException;
import kafka.common.KafkaException;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.utils.IteratorTemplate;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and offset can be applied to the message set
 * which will control the offset into the file and the effective length into the file from which
 * messages will be read
 */
public class FileMessageSet extends MessageSet {

    private static Logger logger = Logger.getLogger(FileMessageSet.class);

    File file;
    FileChannel channel;
    int start;
    int limit;
    boolean initChannelPositionToEnd;

    private AtomicInteger _size ;

    public FileMessageSet(File file, FileChannel channel, int start, int limit, boolean initChannelPositionToEnd) throws IOException{
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.limit = limit;
        this.initChannelPositionToEnd = initChannelPositionToEnd;

        _size = new AtomicInteger((int)(Math.min(channel.size(), limit) - start));

        if (initChannelPositionToEnd) {
            logger.debug(String.format("Creating or reloading log segment %s",file.getAbsolutePath()));
            /* set the file position to the last byte in the file */
            channel.position(channel.size());
        }
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file, FileChannel channel) throws IOException{
        this(file, channel, 0, Integer.MAX_VALUE,true);
    }

    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file) throws IOException{
        this(file, Utils.openChannel(file, true));
    }




    /**
     * Return a message set which is a view into this set starting from the given position and with the given size limit.
     */
    public MessageSet read(int position, int size)  throws IOException{
       return  new FileMessageSet(file,
               channel,
               this.start + position,
                    Math.min(this.start + position + size, sizeInBytes()),
               false);
    }

    /**
     * Write some of this set to the given channel, return the ammount written
     */
    public long writeTo(GatheringByteChannel destChannel, long writePosition, int size) throws IOException {
        // Ensure that the underlying size has not changed.
        int newSize = Math.min((int)channel.size(), limit) - start;
        if (newSize < _size.get()) {
            throw new KafkaException(String
                    .format("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d",file.getAbsolutePath(), _size.get(), newSize));
        }
        long bytesTransferred = channel.transferTo(start + writePosition, Math.min(size, sizeInBytes()), destChannel);
        logger.info("FileMessageSet " + file.getAbsolutePath() + " : bytes transferred : " + bytesTransferred
                + " bytes requested for transfer : " + Math.min(size, sizeInBytes()));
        return  bytesTransferred;
    }


    /**
     * Get an iterator over the messages in the set. We only do shallow iteration here.
     */
    public Iterator<MessageAndOffset> iterator() {
        return new IteratorTemplate<MessageAndOffset>() {
            long location = start;
            public MessageAndOffset makeNext() {
                // read the size of the item
                ByteBuffer sizeOffsetBuffer = ByteBuffer.allocate(12);
                try {
                    channel.read(sizeOffsetBuffer, location);
                }catch (IOException e){
                    logger.error("FileMessageSet iterator read Error",e);
                    throw new InvalidIOException(e.getMessage());
                }
                if(sizeOffsetBuffer.hasRemaining())
                    return allDone();

                sizeOffsetBuffer.rewind();
                long offset = sizeOffsetBuffer.getLong();
                int size = sizeOffsetBuffer.getInt();
                if (size < Message.MinHeaderSize)
                    return allDone();

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try {
                    channel.read(buffer, location + 12);
                }catch (IOException e){
                    logger.error("FileMessageSet iterator read Error",e);
                    throw new InvalidIOException(e.getMessage());
                }
                if(buffer.hasRemaining())
                    return allDone();
                buffer.rewind();

                // increment the location and return the item
                location += size + 12;
                return  new MessageAndOffset(new Message(buffer), offset);
            }
        };
    }

    /**
     * The number of bytes taken up by this file set
     */
    public int sizeInBytes(){
        return _size.get();
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position. If no such offsets are found, return null.
     */
    public  OffsetPosition searchFor(long targetOffset, int startingPosition) throws IOException{
        int position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(MessageSet.LogOverhead);
        int size = _size.get();
        while(position + MessageSet.LogOverhead < size) {
            buffer.rewind();
            channel.read(buffer, position);
            if(buffer.hasRemaining())
                throw new IllegalStateException(String
                        .format("Failed to read complete buffer for targetOffset %d startPosition %d in %s",targetOffset, startingPosition, file.getAbsolutePath()));
            buffer.rewind();
            long offset = buffer.getLong();
            if(offset >= targetOffset)
                return new OffsetPosition(offset, position);
            int messageSize = buffer.getInt();
            position += MessageSet.LogOverhead + messageSize;
        }
       return null;
    }

    /**
     * Append this message to the message set
     */
    public void append(ByteBufferMessageSet messages) throws IOException{
        long written = messages.writeTo(channel, 0, messages.sizeInBytes());
        _size.getAndAdd((int)written);
    }

    /**
     * Commit all written data to the physical disk
     */
    //TODO 异步改同步方便测试
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this message set
     */
    public void close() throws IOException {
        flush();
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     */
    public boolean delete() throws IOException{
        channel.close();
        return file.delete();
    }

    /**
     * Truncate this file message set to the given size. Note that this API does no checking that the
     * given size falls on a valid byte offset.
     */
    public void truncateTo(int targetSize) throws IOException{
        if(targetSize > sizeInBytes())
            throw new KafkaException(String.format(String.format("Attempt to truncate log segment to %d bytes failed since the current ",targetSize) +
                    " size of this log segment is only %d bytes",sizeInBytes()));
        channel.truncate(targetSize);
        channel.position(targetSize);
        _size.set(targetSize);
    }
}

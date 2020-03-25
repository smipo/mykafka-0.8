package kafka.message;

import kafka.common.InvalidIOException;
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
import java.util.concurrent.atomic.AtomicLong;
/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and offset can be applied to the message set
 * which will control the offset into the file and the effective length into the file from which
 * messages will be read
 */
public class FileMessageSet extends MessageSet {

    private static Logger logger = Logger.getLogger(FileMessageSet.class);

    FileChannel channel;
    long offset;
    long limit;
    boolean mutable;
    AtomicBoolean needRecover;


    private AtomicLong setSize = new AtomicLong();
    private AtomicLong setHighWaterMark = new AtomicLong();


    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(FileChannel channel, boolean mutable) throws IOException{
        this(channel, 0, Long.MAX_VALUE, mutable, new AtomicBoolean(false));
    }


    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file, boolean mutable) throws IOException{
        this(Utils.openChannel(file, mutable), mutable);
    }


    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(FileChannel channel, boolean mutable, AtomicBoolean needRecover ) throws IOException{
        this(channel, 0, Long.MAX_VALUE, mutable, needRecover);
    }


    /**
     * Create a file message set with no limit or offset
     */
    public FileMessageSet(File file, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(Utils.openChannel(file, mutable), mutable, needRecover);
    }

    public FileMessageSet(FileChannel channel,long offset,long limit, boolean mutable, AtomicBoolean needRecover) throws IOException{
        this.channel = channel;
        this.offset = offset;
        this.limit = limit;
        this.mutable = mutable;
        this.needRecover = needRecover;

        if(mutable) {
            if(limit < Long.MAX_VALUE || offset > 0)
                throw new IllegalArgumentException("Attempt to open a mutable message set with a view or offset, which is not allowed.");

            if (needRecover.get()) {
                // set the file position to the end of the file for appending messages
                long startMs = System.currentTimeMillis();
                long truncated = recover();
                logger.info("Recovery succeeded in " + (System.currentTimeMillis() - startMs) / 1000 +
                        " seconds. " + truncated + " bytes truncated.");
            }
            else {
                setSize.set(channel.size());
                setHighWaterMark.set(sizeInBytes());
                channel.position(channel.size());
            }
        } else {
            setSize.set(Math.min(channel.size(), limit) - offset);
            setHighWaterMark.set(sizeInBytes());
            logger.debug("initializing high water mark in immutable mode: " + highWaterMark());
        }
    }


    /**
     * Return a message set which is a view into this set starting from the given offset and with the given size limit.
     */
    public MessageSet read(long readOffset, long size)  throws IOException{
       return new FileMessageSet(channel, this.offset + readOffset,Math.min(this.offset + readOffset + size, highWaterMark()),
                false, new AtomicBoolean(false));
    }

    /**
     * Write some of this set to the given channel, return the ammount written
     */
    public long writeTo(GatheringByteChannel destChannel, long writeOffset, long size) throws IOException {
        return channel.transferTo(offset + writeOffset, Math.min(size, sizeInBytes()), destChannel);
    }


    /**
     * Get an iterator over the messages in the set
     */
    public Iterator<MessageAndOffset> iterator() {
        return new IteratorTemplate<MessageAndOffset>() {
            long location = offset;
            public MessageAndOffset makeNext() {
                // read the size of the item
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                try {
                    channel.read(sizeBuffer, location);
                }catch (IOException e){
                    logger.error("FileMessageSet iterator read Error",e);
                    throw new InvalidIOException(e.getMessage());
                }
                if(sizeBuffer.hasRemaining())
                    return allDone();

                sizeBuffer.rewind();
                int size = sizeBuffer.getInt();
                if (size < Message.MinHeaderSize)
                    return allDone();

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try {
                    channel.read(buffer, location + 4);
                }catch (IOException e){
                    logger.error("FileMessageSet iterator read Error",e);
                    throw new InvalidIOException(e.getMessage());
                }
                if(buffer.hasRemaining())
                    return allDone();
                buffer.rewind();

                // increment the location and return the item
                location += size + 4;
                return  new MessageAndOffset(new Message(buffer), location);
            }
        };
    }

    /**
     * The number of bytes taken up by this file set
     */
    public  long sizeInBytes(){
        return setSize.get();
    }

    /**
     * The high water mark
     */
    public long highWaterMark(){
        return setHighWaterMark.get();
    }

    public void checkMutable() {
        if(!mutable)
            throw new IllegalStateException("Attempt to invoke mutation on immutable message set.");
    }

    /**
     * Append this message to the message set
     */
    public void append(MessageSet messages) throws IOException{
        checkMutable();
        long written = 0L;
        while(written < messages.sizeInBytes())
            written += messages.writeTo(channel, 0, messages.sizeInBytes());
        setSize.getAndAdd(written);
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        long startTime = System.currentTimeMillis();
        checkMutable();
        channel.force(true);
        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.debug("flush time " + elapsedTime);
        setHighWaterMark.set(sizeInBytes());
        logger.debug("flush high water mark:" + highWaterMark());
    }

    /**
     * Close this message set
     */
    public void close() throws IOException {
        if(mutable)
            flush();
        channel.close();
    }

    /**
     * Recover log up to the last complete entry. Truncate off any bytes from any incomplete messages written
     */
    public long recover() throws IOException{
        checkMutable();
        long len = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long validUpTo = 0;
        long next = 0L;
        do {
            next = validateMessage(channel, validUpTo, len, buffer);
            if(next >= 0)
                validUpTo = next;
        } while(next >= 0);
        channel.truncate(validUpTo);
        setSize.set(validUpTo);
        setHighWaterMark.set(validUpTo);
        logger.info("recover high water mark:" + highWaterMark());
        /* This should not be necessary, but fixes bug 6191269 on some OSs. */
        channel.position(validUpTo);
        needRecover.set(false);
        return len - validUpTo;
    }

    /**
     * Read, validate, and discard a single message, returning the next valid offset, and
     * the message being validated
     */
    private long  validateMessage(FileChannel channel, long start, long len,ByteBuffer buffer) throws IOException{
        buffer.rewind();
        int read = channel.read(buffer, start);
        if(read < 4)
            return -1;

        // check that we have sufficient bytes left in the file
        int size = buffer.getInt(0);
        if (size < Message.MinHeaderSize)
            return -1;

        long next = start + 4 + size;
        if(next > len)
            return -1;

        // read the message
        ByteBuffer messageBuffer = ByteBuffer.allocate(size);
        long curr = start + 4;
        while(messageBuffer.hasRemaining()) {
            read = channel.read(messageBuffer, curr);
            if(read < 0)
                throw new IllegalStateException("File size changed during recovery!");
            else
                curr += read;
        }
        messageBuffer.rewind();
        Message message = new Message(messageBuffer);
        if(!message.isValid())
            return -1;
        else
            return next;
    }
}

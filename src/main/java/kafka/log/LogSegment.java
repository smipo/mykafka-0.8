package kafka.log;

import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.sun.corba.se.impl.naming.cosnaming.TransientNameServer.trace;

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 */
public class LogSegment {

    FileMessageSet messageSet;
    OffsetIndex index;
    long start;
    int indexIntervalBytes;
    long time;

    public LogSegment(FileMessageSet messageSet, OffsetIndex index, long start, int indexIntervalBytes, long time) {
        this.messageSet = messageSet;
        this.index = index;
        this.start = start;
        this.indexIntervalBytes = indexIntervalBytes;
        this.time = time;

        if (messageSet.sizeInBytes() > 0)
            firstAppendTime = System.currentTimeMillis();
    }
    public LogSegment(File dir,long startOffset, int indexIntervalBytes,int maxIndexSize) throws IOException{
        this(new FileMessageSet(Log.logFilename(dir, startOffset)),
                new OffsetIndex(Log.indexFilename(dir, startOffset), startOffset,  maxIndexSize),
                startOffset,
                indexIntervalBytes,
                System.currentTimeMillis());
    }
    Long firstAppendTime;

    /* the number of bytes since we last added an entry in the offset index */
    int bytesSinceLastIndexEntry = 0;

    volatile boolean deleted = false;

    /* Return the size in bytes of this log segment */
    public long size(){
        return messageSet.sizeInBytes();
    }

    public void updateFirstAppendTime() {
        if (firstAppendTime == null)
            firstAppendTime =  System.currentTimeMillis();
    }

    /**
     * Append the given messages starting with the given offset. Add
     * an entry to the index if needed.
     *
     * It is assumed this method is being called from within a lock
     */
    public void append(long offset, ByteBufferMessageSet messages) throws IOException{
        if (messages.sizeInBytes() > 0) {
            trace(String.format("Inserting %d bytes at offset %d at position %d",messages.sizeInBytes() , offset, messageSet.sizeInBytes()));
            // append an entry to the index (if needed)
            if(bytesSinceLastIndexEntry > indexIntervalBytes) {
                index.append(offset, messageSet.sizeInBytes());
                this.bytesSinceLastIndexEntry = 0;
            }
            // append the messages
            messageSet.append(messages);
            updateFirstAppendTime();
            this.bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    /**
     * Find the physical file position for the least offset >= the given offset. If no offset is found
     * that meets this criteria before the end of the log, return null.
     */
    private OffsetPosition translateOffset(long offset) throws IOException{
        OffsetPosition mapping = index.lookup(offset);
        return messageSet.searchFor(offset, mapping.position());
    }

    /**
     * Read a message set from this segment beginning with the first offset
     * greater than or equal to the startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     */
    public MessageSet read(long startOffset, int maxSize, Long maxOffset) throws IOException {
        if(maxSize < 0)
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)",maxSize));
        if(maxSize == 0)
            return MessageSet.Empty;

        int logSize = messageSet.sizeInBytes(); // this may change, need to save a consistent copy
        OffsetPosition startPosition = translateOffset(startOffset);

        // if the start position is already off the end of the log, return MessageSet.Empty
        if(startPosition == null)
            return MessageSet.Empty;

        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        int length ;
        if(maxOffset == null){
            length = maxSize;
        }else{
            // there is a max offset, translate it to a file position and use that to calculate the max read size
            if(maxOffset < startOffset)
                throw new IllegalArgumentException(String.format("Attempt to read with a maximum offset (%d) less than the start offset (%d).",maxOffset, startOffset));
            OffsetPosition mapping = translateOffset(maxOffset);
            int endPosition ;
            if(mapping == null)
                endPosition = logSize; // the max offset is off the end of the log, use the end of the file
            else
                endPosition =  mapping.position;
            length = Math.min(endPosition - startPosition.position, maxSize);
        }
        return messageSet.read(startPosition.position, length);
    }

    @Override
    public String toString() {
        return "LogSegment(start=" + start + ", size=" + size() + ")";
    }


    /**
     * Truncate off all index and log entries with offsets greater than or equal to the current offset.
     */
    public void truncateTo(long offset) throws IOException{
        OffsetPosition mapping = translateOffset(offset);
        if(mapping == null)
            return ;
        index.truncateTo(offset);
        // after truncation, reset and allocate more space for the (new currently  active) index
        index.resize(index.maxIndexSize);
        messageSet.truncateTo(mapping.position);
        if (messageSet.sizeInBytes() == 0)
            firstAppendTime = null;
        bytesSinceLastIndexEntry = 0;
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     */
    public long nextOffset() throws IOException{
        MessageSet ms = read(index.lastOffset, messageSet.sizeInBytes(), null);
        if(ms == null)  return start;
        Iterator<MessageAndOffset> iterator = ms.iterator();
        MessageAndOffset messageAndOffset = null;
        while (iterator.hasNext()){
            messageAndOffset = iterator.next();
        }
        if(messageAndOffset == null)  return start;
        else  return messageAndOffset.nextOffset();
    }

    /**
     * Flush this log segment to disk
     */
    public void flush()throws IOException {
        messageSet.flush();
        index.flush();
    }

    /**
     * Close this log segment
     */
    public void close() throws IOException {
        index.close();
        messageSet.close();
    }
}

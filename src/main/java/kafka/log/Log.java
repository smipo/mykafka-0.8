package kafka.log;

import kafka.api.OffsetRequest;
import kafka.common.*;
import kafka.message.*;
import kafka.utils.Pair;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static kafka.utils.Preconditions.*;

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 */
public class Log {

    private static Logger logger = Logger.getLogger(Log.class);

    public static String LogFileSuffix = ".log";
    public static String IndexFileSuffix = ".index";


    /**
     * Find a given range object in a list of ranges by a value in that range. Does a binary search over the ranges
     * but instead of checking for equality looks within the range. Takes the array size as an option in case
     * the array grows while searching happens
     *
     * TODO: This should move into SegmentList.scala
     */
    public static LogSegment findRange(LogSegment[] ranges, long value, int arraySize){
        if(ranges.length < 1)
            return null;

        // check out of bounds
        if(value < ranges[0].start)
            return null;

        int low = 0;
        int high = arraySize - 1;
        while(low <= high) {
            int mid = (high + low) / 2;
            LogSegment found = ranges[mid];
            if(found.start == value)
                return found;
            else if (value < found.start)
                high = mid - 1;
            else
                low = mid;
        }
        return ranges[low];
    }


    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically
     */
    public static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static File logFilename(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    public static File indexFilename(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }

    public static Long getEmptyOffsets(long timestamp) {
        if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
            return 0L;
        else return null;
    }

    private File dir;
    private long maxLogFileSize;
    private int maxMessageSize;
    private int flushInterval;
    private long rollIntervalMs;
    private boolean needsRecovery;
    private long milliseconds;

    private int maxIndexSize = (10*1024*1024);
    private int indexIntervalBytes = 4096;
    int brokerId;

    public Log(File dir, long maxLogFileSize, int maxMessageSize, int flushInterval, long rollIntervalMs, boolean needsRecovery,
               long milliseconds, int maxIndexSize, int indexIntervalBytes, int brokerId) throws IOException{
        this.dir = dir;
        this.maxLogFileSize = maxLogFileSize;
        this.maxMessageSize = maxMessageSize;
        this.flushInterval = flushInterval;
        this.rollIntervalMs = rollIntervalMs;
        this.needsRecovery = needsRecovery;
        this.milliseconds = milliseconds;
        this.maxIndexSize = maxIndexSize;
        this.indexIntervalBytes = indexIntervalBytes;
        this.brokerId = brokerId;

        segments = loadSegments();
        nextOffset = new AtomicLong(segments.last().nextOffset());
        name  = dir.getName();
    }

    /* A lock that guards all modifications to the log */
    private Object lock = new Object();

    /* The current number of unflushed messages appended to the write */
    private AtomicInteger unflushed = new AtomicInteger(0);

    /* last time it was flushed */
    private AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());

    /* The actual segments of the log */
    private  SegmentList<LogSegment> segments;

    /* Calculate the offset of the next message */
    private AtomicLong nextOffset ;

    /* The name of this log */
    String name ;

    public File dir() {
        return dir;
    }

    protected SegmentList<LogSegment> segments() {
        return segments;
    }

    /* Load the log segments from the log files on disk */
    private SegmentList<LogSegment> loadSegments() throws IOException{
        // open all the segments read-only
        ArrayList<LogSegment> logSegments = new ArrayList<>();
        File[] ls = dir.listFiles();
        if(ls != null) {
            for(File file:ls){
                String filename = file.getName();
                if(file.isFile()){
                    if(!file.canRead()){
                        throw new InvalidIOException("Could not read file " + file);
                    }else if(filename.endsWith(IndexFileSuffix)) {
                        // ensure that we have a corresponding log file for this index file
                        File log = new File(file.getAbsolutePath().replace(IndexFileSuffix, LogFileSuffix));
                        if(!log.exists()) {
                            logger.warn(String.format("Found an orphaned index file, %s, with no corresponding log file.",file.getAbsolutePath()));
                            file.delete();
                        }
                    } else if(filename.endsWith(LogFileSuffix)) {
                        long offset = Long.parseLong(filename.substring(0, filename.length() - LogFileSuffix.length()));
                        // TODO: we should ideally rebuild any missing index files, instead of erroring out
                        if(!Log.indexFilename(dir, offset).exists())
                            throw new IllegalStateException("Found log file with no corresponding index file.");
                        logSegments.add(new LogSegment( dir,
                                offset,
                                indexIntervalBytes,
                                maxIndexSize));
                    }
                }
            }
        }

        if(logSegments.size() == 0) {
            // no existing segments, create a new mutable segment
            logSegments.add(new LogSegment( dir,
                    0,
                    indexIntervalBytes,
                    maxIndexSize));
        } else {
            // there is at least one existing segment, validate and recover them/it
            // sort segments into ascending order for fast searching
            Collections.sort(logSegments, new Comparator<LogSegment>() {
                public int compare(LogSegment s1,LogSegment s2)  {
                    if(s1.start == s2.start) return 0;
                    else if(s1.start < s2.start) return -1;
                    else return 1;
                }
            });
            // reset the index size of the last (current active) log segment to its maximum value
            logSegments.get(logSegments.size() - 1).index.resize(maxIndexSize);

            // run recovery on the last segment if necessary
            if(needsRecovery) {
                LogSegment activeSegment = logSegments.get(logSegments.size() - 1);
                try {
                    recoverSegment(activeSegment);
                } catch(InvalidOffsetException e) {
                    long startOffset = activeSegment.start;
                    logger.warn("Found invalid offset during recovery of the active segment for topic partition " + dir.getName() +". Deleting the segment and " +
                            "creating an empty one with starting offset " + startOffset);
                    // truncate the active segment to its starting offset
                    activeSegment.truncateTo(startOffset);
                }
            }
        }

        // Check for the index file of every segment, if it's empty or its last offset is greater than its base offset.
        for (LogSegment s : logSegments) {
            checkArgument(s.index.entries() == 0 || s.index.lastOffset > s.index.baseOffset,
                           String.format("Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
                                   ,s.index.file.getAbsolutePath(), s.index.lastOffset, s.index.baseOffset));
        }

        return  new SegmentList(logSegments);
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log.
     */
    private void recoverSegment(LogSegment segment) throws IOException{
        logger.info(String.format("Recovering log segment %s",segment.messageSet.file.getAbsolutePath()));
        segment.index.truncate();
        int validBytes = 0;
        int lastIndexEntry = 0;
        Iterator<MessageAndOffset> iter = segment.messageSet.iterator();
        try {
            while(iter.hasNext()) {
                MessageAndOffset entry = iter.next();
                entry.message().ensureValid();
                if(validBytes - lastIndexEntry > indexIntervalBytes) {
                    // we need to decompress the message, if required, to get the offset of the first uncompressed message
                    long startOffset ;
                    if( entry.message().compressionCodec() instanceof NoCompressionCodec ){
                        startOffset = entry.offset();
                    }else{
                        Iterator<MessageAndOffset>  iterator = ByteBufferMessageSet.decompress(entry.message()).iterator();
                        startOffset = iterator.next().offset();
                    }
                    segment.index.append(startOffset, validBytes);
                    lastIndexEntry = validBytes;
                }
                validBytes += MessageSet.entrySize(entry.message());
            }
        } catch (InvalidMessageException e){
                logger.warn("Found invalid messages in log " + name);
        }
        int truncated = segment.messageSet.sizeInBytes() - validBytes;
        if(truncated > 0)
            logger.warn("Truncated " + truncated + " invalid bytes from the log " + name + ".");
        segment.messageSet.truncateTo(validBytes);
    }



    /**
     * The number of segments in the log
     */
    public int numberOfSegments(){
        return segments.view().size();
    }

    /**
     * Close this log
     */
    public void close() throws IOException{
        synchronized(lock) {
            for(LogSegment seg : segments.view())
                seg.messageSet.close();
        }
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     *
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * Returns a tuple containing (first_offset, last_offset) for the newly appended of the message set,
     * or (-1,-1) if the message set is empty
     */
    public Pair<Long,Long> append(ByteBufferMessageSet messages,boolean assignOffsets) {
        MessageSetAppendInfo messageSetInfo = analyzeAndValidateMessageSet(messages);

        // if we have any valid messages, append them to the log
        if(messageSetInfo.count == 0) {
           return new Pair<>(-1L, -1L);
        } else {
            // trim any invalid bytes or partial messages before appending it to the on-disk log
            ByteBufferMessageSet validMessages = trimInvalidBytes(messages);

            try {
                // they are valid, insert them in the log
                Pair<Long,Long> offsets ;
                synchronized(lock) {
                    long firstOffset = nextOffset.get();

                    // maybe roll the log if this segment is full
                    LogSegment segment = maybeRoll(segments.last());

                    // assign offsets to the messageset
                    long lastOffset;
                    if(assignOffsets) {
                        AtomicLong offsetCounter = new AtomicLong(nextOffset.get());
                        try {
                            validMessages = validMessages.assignOffsets(offsetCounter, messageSetInfo.codec);
                        } catch (IOException e){
                            throw new KafkaException(String.format("Error in validating messages while appending to log '%s'",name), e);
                        }
                        long assignedLastOffset = offsetCounter.get() - 1;
                        long numMessages = assignedLastOffset - firstOffset + 1;
                        lastOffset = assignedLastOffset;
                    } else {
                        checkArgument(messageSetInfo.offsetsMonotonic, "Out of order offsets found in " + messages);
                        checkArgument(messageSetInfo.firstOffset >= nextOffset.get(),
                                   String.format("Attempt to append a message set beginning with offset %d to a log with log end offset %d."
                                           ,messageSetInfo.firstOffset, nextOffset.get()));
                        lastOffset = messageSetInfo.lastOffset;
                    }

                    // Check if the message sizes are valid. This check is done after assigning offsets to ensure the comparison
                    // happens with the new message size (after re-compression, if any)
                    Iterator<MessageAndOffset> iterator = validMessages.shallowIterator();
                    while (iterator.hasNext()){
                        MessageAndOffset  messageAndOffset = iterator.next();
                        if(MessageSet.entrySize(messageAndOffset.message()) > maxMessageSize)
                            throw new MessageSizeTooLargeException(String
                                    .format("Message size is %d bytes which exceeds the maximum configured message size of %d.",MessageSet.entrySize(messageAndOffset.message()) , maxMessageSize));
                    }

                    // now append to the log
                    segment.append(firstOffset, validMessages);

                    // advance the log end offset
                    nextOffset.set(lastOffset + 1);

                    logger.info(String
                            .format("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s",this.name, firstOffset, nextOffset.get(), validMessages));

                    // return the offset at which the messages were appended
                    offsets = new Pair<> (firstOffset, lastOffset);
                }

                // maybe flush the log and index
                int numAppendedMessages = (int)(offsets.getValue() - offsets.getKey() + 1);
                maybeFlush(numAppendedMessages);

                // return the first and last offset
                return offsets;
            } catch (IOException e){
                throw new KafkaStorageException(String.format("I/O exception in append to log '%s'",name), e);
            }
        }
    }

    class MessageSetAppendInfo{
        long firstOffset;
        long lastOffset;
        CompressionCodec codec;
        int count;
        boolean offsetsMonotonic;

        public MessageSetAppendInfo(long firstOffset, long lastOffset, CompressionCodec codec, int count, boolean offsetsMonotonic) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.codec = codec;
            this.count = count;
            this.offsetsMonotonic = offsetsMonotonic;
        }
    }

    /**
     * Validate the following:
     * 1. each message matches its CRC
     *
     * Also compute the following quantities:
     * 1. First offset in the message set
     * 2. Last offset in the message set
     * 3. Number of messages
     * 4. Whether the offsets are monotonically increasing
     * 5. Whether any compression codec is used (if many are used, then the last one is given)
     */
    private MessageSetAppendInfo analyzeAndValidateMessageSet(ByteBufferMessageSet messages) {
        int messageCount = 0;
        long firstOffset = -1L, lastOffset = -1L;
        CompressionCodec codec = new NoCompressionCodec();
        boolean monotonic = true;
        Iterator<MessageAndOffset> iterator = messages.shallowIterator();
        while (iterator.hasNext()){
            MessageAndOffset messageAndOffset = iterator.next();
            // update the first offset if on the first message
            if(firstOffset < 0)
                firstOffset = messageAndOffset.offset();
            // check that offsets are monotonically increasing
            if(lastOffset >= messageAndOffset.offset())
                monotonic = false;
            // update the last offset seen
            lastOffset = messageAndOffset.offset();

            // check the validity of the message by checking CRC
            Message m = messageAndOffset.message();
            m.ensureValid();

            messageCount += 1;

            CompressionCodec messageCodec = m.compressionCodec();
            if(messageCodec instanceof NoCompressionCodec){

            }else{
                codec = messageCodec;
            }
        }
       return new MessageSetAppendInfo(firstOffset, lastOffset, codec, messageCount, monotonic);
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     */
    private ByteBufferMessageSet trimInvalidBytes(ByteBufferMessageSet messages) {
        long messageSetValidBytes = messages.validBytes();
        if(messageSetValidBytes < 0)
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        if(messageSetValidBytes == messages.sizeInBytes()) {
           return messages;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = messages.buffer().duplicate();
            validByteBuffer.limit((int)messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }

    /**
     * Read a message set from the log.
     * startOffset - The logical offset to begin reading at
     * maxLength - The maximum number of bytes to read
     * maxOffset - The first offset not included in the read
     */
    public MessageSet read(long startOffset, int maxLength, Long maxOffset) throws IOException {
        logger.trace(String.format("Reading %d bytes from offset %d in log %s of length %d bytes",maxLength, startOffset, name, size()));
        List<LogSegment> view = segments.view();

        // check if the offset is valid and in range
        long first = view.get(0).start;
        long next = nextOffset.get();
        if(startOffset == next)
            return MessageSet.Empty;
        else if(startOffset > next || startOffset < first)
            throw new OffsetOutOfRangeException(String.format("Request for offset %d but we only have log segments in the range %d to %d.",startOffset , first, next));

        // Do the read on the segment with a base offset less than the target offset
        // TODO: to handle sparse offsets, we need to skip to the next segment if this read doesn't find anything
        LogSegment[] ranges = new LogSegment[view.size()] ;
        LogSegment segment = Log.findRange(view.toArray(ranges), startOffset, view.size());
        if(segment == null){
            throw new OffsetOutOfRangeException("Offset is earlier than the earliest offset");
        }else{
           return segment.read(startOffset, maxLength, maxOffset);
        }
    }

    /**
     * Delete any log segments matching the given predicate function
     */
    public LogSegment[] markDeletedWhile(LogSegment[] deletable) throws IOException{
        synchronized(lock) {
            List<LogSegment> view = segments.view();
            for(LogSegment seg : deletable)
                seg.deleted = true;
            int numToDelete = deletable.length;
            // if we are deleting everything, create a new empty segment
            if(numToDelete == view.size()) {
                if (view.get(numToDelete - 1).size() > 0)
                    roll();
                else {
                    // If the last segment to be deleted is empty and we roll the log, the new segment will have the same
                    // file name. So simply reuse the last segment and reset the modified time.
                    view.get(numToDelete - 1).messageSet.file.setLastModified(System.currentTimeMillis());
                    numToDelete -= 1;
                }
            }
            List<LogSegment> list = segments.trunc(numToDelete);
            LogSegment[] logSegments = new LogSegment[list.size()];
            return list.toArray(logSegments);
        }
    }

    /**
     * Get the size of the log in bytes
     */
    public long size(){
        return  segments.view().get(0).size();
    }


    /**
     *  Get the offset of the next message that will be appended
     */
    public long logEndOffset(){
        return nextOffset.get();
    }

    /**
     * Roll the log over if necessary
     */
    private LogSegment maybeRoll(LogSegment segment) throws IOException{
        if(segment.messageSet.sizeInBytes() > maxLogFileSize) {
            logger.info(String.format("Rolling %s due to full data log",name));
            return  roll();
        } else if((segment.firstAppendTime == null) && (milliseconds - segment.firstAppendTime > rollIntervalMs)) {
            logger.info(String.format("Rolling %s due to time based rolling",name));
            return roll();
        } else if(segment.index.isFull()) {
            logger.info(String
                    .format("Rolling %s due to full index maxIndexSize = %d, entries = %d, maxEntries = %d",name, segment.index.maxIndexSize, segment.index.entries(), segment.index.maxEntries));
            return roll();
        } else
            return segment;
    }

    /**
     * Create a new segment and make it active
     */
    public LogSegment roll() throws IOException{
        synchronized (lock){
            flush();
            return rollToOffset(logEndOffset());
        }
    }

    /**
     * Roll the log over to the given new offset value
     */
    private LogSegment rollToOffset(long newOffset) throws IOException{
        File logFile = logFilename(dir, newOffset);
        if(logFile.exists()){
            logger.warn("Newly rolled segment file " + logFile.getAbsolutePath() + " already exists; deleting it first");
            logFile.delete();
        }
        File indexFile = indexFilename(dir, newOffset);
        if(indexFile.exists()){
            logger.warn("Newly rolled segment file " + indexFile.getAbsolutePath() + " already exists; deleting it first");
            indexFile.delete();
        }
        logger.info("Rolling log '" + name + "' to " + logFile.getAbsolutePath() + " and " + indexFile.getAbsolutePath());

        segments.view().get(segments.view().size() - 1).index.trimToValidSize();

        List<LogSegment> segmentsView = segments.view();
        if(segmentsView.size() > 0 && segmentsView.get(segmentsView.size() - 1).start == newOffset)
            throw new KafkaException(String.format("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists",dir.getName(), newOffset));

        LogSegment segment = new LogSegment(dir,
                newOffset,
                indexIntervalBytes,
                maxIndexSize);
        segments.append(segment);
        return segment;
    }

    /**
     * Flush the log if necessary
     */
    private void maybeFlush(int numberOfMessages) throws IOException{
        if(unflushed.addAndGet(numberOfMessages) >= flushInterval) {
            flush();
        }
    }

    /**
     * Flush this log file to the physical disk
     */
    public void flush()  throws IOException{
        if (unflushed.get() == 0) return;

        synchronized(lock) {
            logger.debug("Flushing log '" + name + "' last flushed: " + getLastFlushedTime() + " current time: " +
                    System.currentTimeMillis());
            segments.last().messageSet.flush();
            unflushed.set(0);
            lastflushedTime.set(System.currentTimeMillis());
        }
    }

    public long[]  getOffsetsBefore(long timestamp,int maxNumOffsets) {
        List<LogSegment> segsArray = segments.view();
        Pair<Long,Long>[] offsetTimeArray = null;
        if (segments.last().size() > 0)
            offsetTimeArray = new Pair[segsArray.size() + 1];
        else
            offsetTimeArray = new Pair[segsArray.size()];

        for (int i = 0;i < segsArray.size();i++)
            offsetTimeArray[i] = new Pair<>(segsArray.get(i).start, segsArray.get(i).messageSet.file.lastModified());
        if (segments.last().size() > 0)
            offsetTimeArray[segsArray.size()] = new Pair<>(logEndOffset(), System.currentTimeMillis());

        int startIndex = -1;
        if(timestamp == OffsetRequest.LatestTime )
            startIndex = offsetTimeArray.length - 1;
        else if(timestamp == OffsetRequest.EarliestTime)
            startIndex = 0;
        else{
            boolean isFound = false;
            startIndex = offsetTimeArray.length - 1;
            while (startIndex >= 0 && !isFound) {
                if (offsetTimeArray[startIndex].getValue() <= timestamp)
                    isFound = true;
                else
                    startIndex -=1;
            }
        }
        //Todo
        int retSize = Math.min(maxNumOffsets,startIndex + 1) ;
        long[] ret = new long[retSize];
        for (int j = 0;j < retSize;j++) {
            ret[j] = offsetTimeArray[startIndex].getKey();
            startIndex -= 1;
        }
        return ret;
    }
    public void delete()  throws IOException{
        deleteSegments(segments.view());
        Utils.rm(dir);
    }


    /* Attempts to delete all provided segments from a log and returns how many it was able to */
    public int deleteSegments(List<LogSegment> segments) throws IOException{
        int total = 0;
        for(LogSegment segment : segments) {
            logger.info("Deleting log segment " + segment.start + " from " + name);
            boolean deletedLog = segment.messageSet.delete();
            boolean deletedIndex = segment.index.delete();
            if(!deletedIndex || !deletedLog) {
                throw new KafkaStorageException("Deleting log segment " + segment.start + " failed.");
            } else {
                total += 1;
            }
            if(segment.messageSet.file.exists())
                logger.error("Data log file %s still exists".format(segment.messageSet.file.getAbsolutePath()));
            if(segment.index.file.exists())
                logger.error("Index file %s still exists".format(segment.index.file.getAbsolutePath()));
        }
        return total;
    }

    public void truncateTo(long targetOffset)throws IOException {
        if(targetOffset < 0)
            throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset + ""));
         synchronized(lock) {
            List<LogSegment> segmentsToBeDeleted = segments.view().stream().filter(segment -> segment.start > targetOffset).collect(Collectors.toList());
            int viewSize = segments.view().size();
             int numSegmentsDeleted = deleteSegments(segmentsToBeDeleted);
            /* We should not hit this error because segments.view is locked in markedDeletedWhile() */
            if(numSegmentsDeleted != segmentsToBeDeleted.size())
                logger.error("Failed to delete some segments when attempting to truncate to offset " + targetOffset +")");
            if (numSegmentsDeleted == viewSize) {
                segments.trunc(segments.view().size());
                rollToOffset(targetOffset);
                this.nextOffset.set(targetOffset);
            } else {
                if(targetOffset > logEndOffset()) {
                    logger.error("Target offset %d cannot be greater than the last message offset %d in the log %s".
                            format(targetOffset + "", logEndOffset(), segments.last().messageSet.file.getAbsolutePath()));
                } else {
                    // find the log segment that has this hw
                    LogSegment[] ranges = new LogSegment[segments.view().size()];
                    LogSegment segmentToBeTruncated = findRange(segments.view().toArray(ranges), targetOffset,segments.view().size());
                    if(segmentToBeTruncated != null){
                        int truncatedSegmentIndex = segments.view().indexOf(segmentToBeTruncated);
                        segments.truncLast(truncatedSegmentIndex);
                        segmentToBeTruncated.truncateTo(targetOffset);
                        this.nextOffset.set(targetOffset);
                        logger.info("Truncated log segment %s to target offset %d".format(segments.last().messageSet.file.getAbsolutePath(), targetOffset));
                    }
                }
            }
        }
    }

    /**
     *  Truncate all segments in the log and start a new segment on a new offset
     */
    public void truncateAndStartWithNewOffset(long newOffset) throws IOException{
         synchronized(lock) {
            List<LogSegment> deletedSegments = segments.trunc(segments.view().size());
            logger.info("Truncate and start log '" + name + "' to " + newOffset);
            deleteSegments(deletedSegments);
            segments.append(new LogSegment(dir,
                    newOffset,
                    indexIntervalBytes,
                    maxIndexSize));
            this.nextOffset.set(newOffset);
        }
    }
    public String topicName() {
        return name.substring(0, name.lastIndexOf("-"));
    }

    public String getTopicName() {
        return name.substring(0, name.lastIndexOf("-"));
    }

    public long getLastFlushedTime() {
        return lastflushedTime.get();
    }
}

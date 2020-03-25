package kafka.log;

import kafka.api.OffsetRequest;
import kafka.common.InvalidIOException;
import kafka.common.InvalidMessageSizeException;
import kafka.common.OffsetOutOfRangeException;
import kafka.message.*;
import kafka.utils.Pair;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.w3c.dom.ranges.Range;

import javax.swing.text.html.Option;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

/**
 * An append-only log for storing messages.
 */
public class Log {

    private static Logger logger = Logger.getLogger(Log.class);

    public static final String FileSuffix = ".kafka";


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
        if(value < ranges[0].start || value > ranges[arraySize - 1].start + ranges[arraySize - 1].size())
            throw new OffsetOutOfRangeException("offset " + value + " is out of range");

        // check at the end
        if (value == ranges[arraySize - 1].start + ranges[arraySize - 1].size())
            return null;

        int low = 0;
        int high = arraySize - 1;
        while(low <= high) {
            int mid = (high + low) / 2;
            LogSegment found = ranges[mid];
            if (value < found.start) {
                high = mid - 1;
            }else{
                FileMessageSet fileMessageSet = found.messageSet;
                Iterator<MessageAndOffset> iterator = fileMessageSet.iterator();
                while (iterator.hasNext()){
                    MessageAndOffset messageAndOffset = iterator.next();
                    if(messageAndOffset.offset() == value) return found;
                }
                low = mid + 1;
            }
        }
        return null;
    }


    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically
     */
    public static String nameFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + Log.FileSuffix;
    }

    public static long[] getEmptyOffsets(OffsetRequest request) {

        if (request.time == OffsetRequest.LatestTime || request.time == OffsetRequest.EarliestTime){
            long[] ret = new long[1];
            ret[0] = 0l;
            return ret;
        }
        return new long[0];
    }

    private File dir;
    private long maxSize;
    private int maxMessageSize;
    private int flushInterval;
    private long rollIntervalMs;
    private boolean needRecovery;
    private long milliseconds;


    /* A lock that guards all modifications to the log */
    private Object lock = new Object();

    /* The current number of unflushed messages appended to the write */
    private AtomicInteger unflushed = new AtomicInteger(0);

    /* last time it was flushed */
    private AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());

    /* The actual segments of the log */
    private  SegmentList<LogSegment> segments;

    /* The name of this log */
    String name ;

    public Log(File dir,
               long milliseconds,
               long maxSize,
               int maxMessageSize,
               int flushInterval,
               long rollIntervalMs,
               boolean needRecovery) throws IOException{
        this.dir = dir;
        this.maxSize = maxSize;
        this.maxMessageSize = maxMessageSize;
        this.flushInterval = flushInterval;
        this.rollIntervalMs = rollIntervalMs;
        this.needRecovery = needRecovery;
        this.name = dir.getName();
        this.milliseconds = milliseconds;
        segments = loadSegments();
    }

    public File dir() {
        return dir;
    }

    protected SegmentList<LogSegment> segments() {
        return segments;
    }

    /* Load the log segments from the log files on disk */
    private SegmentList<LogSegment> loadSegments() throws IOException{
        // open all the segments read-only
        ArrayList<LogSegment> accum = new ArrayList<LogSegment>();
        File[] ls = dir.listFiles();
        if(ls != null) {
            for(File file:ls){
                if(file.isFile() && file.toString().endsWith(Log.FileSuffix)){
                    if(!file.canRead()){
                        throw new InvalidIOException("Could not read file " + file);
                    }
                    String filename = file.getName();
                    long start = Long.parseLong(filename.substring(0, filename.length() - Log.FileSuffix.length()));
                    FileMessageSet messageSet = new FileMessageSet(file, false);
                    accum.add(new LogSegment(file, messageSet, start));
                }
            }
        }

        if(accum.size() == 0) {
            // no existing segments, create a new mutable segment
            File newFile = new File(dir, Log.nameFromOffset(0));
            FileMessageSet set = new FileMessageSet(newFile, true);
            accum.add(new LogSegment(newFile, set, 0));
        } else {
            // there is at least one existing segment, validate and recover them/it
            // sort segments into ascending order for fast searching
            Collections.sort(accum, new Comparator<LogSegment>() {
                public int compare(LogSegment s1,LogSegment s2)  {
                    if(s1.start == s2.start) return 0;
                    else if(s1.start < s2.start) return -1;
                    else return 1;
                }
            });
            validateSegments(accum);

            //make the final section mutable and run recovery on it if necessary
            LogSegment last = accum.remove(accum.size() - 1);
            last.messageSet.close();
            logger.info("Loading the last segment " + last.file.getAbsolutePath() + " in mutable mode, recovery " + needRecovery);
            LogSegment mutable = new LogSegment(last.file,  new FileMessageSet(last.file, true, new AtomicBoolean(needRecovery)), last.start);
            accum.add(mutable);
        }
        return new SegmentList(accum);
    }

    /**
     * Check that the ranges and sizes add up, otherwise we have lost some data somewhere
     */
    private void validateSegments(ArrayList<LogSegment> segments) {
        synchronized(lock) {
            for(int i = 0;i < segments.size() - 1;i++){
                LogSegment curr = segments.get(i);
                LogSegment next = segments.get(i+1);
                if(curr.start + curr.size() != next.start)
                    throw new IllegalStateException("The following segments don't validate: " +
                            curr.file.getAbsolutePath() + ", " + next.file.getAbsolutePath());
            }
        }
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
     * Returns the offset at which the messages are written.
     */
    public void append(ByteBufferMessageSet messages) {
        // validate the messages
        messages.verifyMessageSize(maxMessageSize);
        int numberOfMessages = 0;
        Iterator<MessageAndOffset> iterator = messages.iterator();
        while (iterator.hasNext()){
            MessageAndOffset messageAndOffset = iterator.next();
            if(!messageAndOffset.message().isValid())
                throw new InvalidMessageException();
            numberOfMessages += 1;
        }
        //Todo
        // BrokerTopicStat.getBrokerTopicStat(getTopicName).recordMessagesIn(numberOfMessages);
        // BrokerTopicStat.getBrokerAllTopicStat.recordMessagesIn(numberOfMessages);

        // truncate the message set's buffer upto validbytes, before appending it to the on-disk log
        ByteBuffer validByteBuffer = messages.getBuffer().duplicate();
        long messageSetValidBytes = messages.validBytes();
        if(messageSetValidBytes > Integer.MAX_VALUE || messageSetValidBytes < 0)
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes +
                    " Message set cannot be appended to log. Possible causes are corrupted produce requests");

        validByteBuffer.limit((int)messageSetValidBytes);
        ByteBufferMessageSet validMessages = new ByteBufferMessageSet(validByteBuffer);

        // they are valid, insert them in the log
        synchronized(lock) {
            try {
                LogSegment segment = segments.last();
                maybeRoll(segment);
                segment = segments.last();
                segment.append(validMessages);
                maybeFlush(numberOfMessages);
            } catch(IOException e) {
                logger.error("Halting due to unrecoverable I/O error while handling producer request", e);
                Runtime.getRuntime().halt(1);
            }
        }
    }


    /**
     * Read from the log file at the given offset
     */
    public MessageSet read(long offset, int length) throws IOException {
        List<LogSegment> view = segments.view();
        LogSegment[] logSegments = new LogSegment[view.size()];
        LogSegment logSegment = Log.findRange(view.toArray(logSegments), offset, view.size());
        if(logSegment == null) return new ByteBufferMessageSet(ByteBuffer.allocate(0));
        return logSegment.messageSet.read((offset - logSegment.start), length);
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
                    view.get(numToDelete - 1).file.setLastModified(System.currentTimeMillis());
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
     * The byte offset of the message that will be appended next.
     */
    public long nextAppendOffset() throws IOException{
        flush();
        LogSegment last = segments.last();
        return last.start + last.size();
    }

    /**
     *  get the current high watermark of the log
     */
    public long getHighwaterMark(){
        return segments.last().messageSet.highWaterMark();
    }

    /**
     * Roll the log over if necessary
     */
    private void maybeRoll(LogSegment segment) throws IOException{
        if((segment.messageSet.sizeInBytes() > maxSize) ||
                ((segment.firstAppendTime != null) && (milliseconds - segment.firstAppendTime > rollIntervalMs)))
            roll();
    }

    /**
     * Create a new segment and make it active
     */
    public void roll() throws IOException{
        synchronized (lock){
            long newOffset = nextAppendOffset();
            File newFile = new File(dir, Log.nameFromOffset(newOffset));
            if (newFile.exists()) {
                logger.warn("newly rolled logsegment " + newFile.getName() + " already exists; deleting it first");
                newFile.delete();
            }
            logger.debug("Rolling log '" + name + "' to " + newFile.getName());
            segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset));
        }
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

    public long[]  getOffsetsBefore(OffsetRequest request) {
        List<LogSegment> segsArray = segments.view();
        Pair<Long,Long>[] offsetTimeArray = null;
        if (segments.last().size() > 0)
            offsetTimeArray = new Pair[segsArray.size() + 1];
        else
            offsetTimeArray = new Pair[segsArray.size()];

        for (int i = 0;i < segsArray.size();i++)
            offsetTimeArray[i] = new Pair<>(segsArray.get(i).start, segsArray.get(i).file.lastModified());
        if (segments.last().size() > 0)
            offsetTimeArray[segsArray.size()] = new Pair<>(segments.last().start + segments.last().messageSet.highWaterMark(), System.currentTimeMillis());

        int startIndex = -1;
        if(request.time == OffsetRequest.LatestTime )
            startIndex = offsetTimeArray.length - 1;
        else if(request.time == OffsetRequest.EarliestTime)
            startIndex = 0;
        else{
            boolean isFound = false;
            startIndex = offsetTimeArray.length - 1;
            while (startIndex >= 0 && !isFound) {
                if (offsetTimeArray[startIndex].getValue() <= request.time)
                    isFound = true;
                else
                    startIndex -=1;
            }
        }
        //Todo
        int retSize = Math.min(request.maxNumOffsets,startIndex + 1) ;
        long[] ret = new long[retSize];
        for (int j = 0;j < retSize;j++) {
            ret[j] = offsetTimeArray[startIndex].getKey();
            startIndex -= 1;
        }
        return ret;
    }

    public String getTopicName() {
        return name.substring(0, name.lastIndexOf("-"));
    }

    public long getLastFlushedTime() {
        return lastflushedTime.get();
    }

    /**
     * A segment file in the log directory. Each log semgment consists of an open message set, a start offset and a size
     */
    class LogSegment{
        File file;
        FileMessageSet messageSet;
        long start;

        Long firstAppendTime;
        volatile boolean deleted = false;

        public LogSegment(File file, FileMessageSet messageSet,long start){
            this.file = file;
            this.messageSet = messageSet;
            this.start = start;
        }


        public long size(){
            return messageSet.highWaterMark();
        }

        private void updateFirstAppendTime() {
            if (firstAppendTime == null)
                firstAppendTime = System.currentTimeMillis();
        }

        public void append(ByteBufferMessageSet messages) throws IOException {
            if (messages.sizeInBytes() > 0) {
                messageSet.append(messages);
                updateFirstAppendTime();
            }
        }
        @Override
        public String toString(){
            return "(file=" + file + ", start=" + start + ", size=" + size() + ")";
        }
    }
}

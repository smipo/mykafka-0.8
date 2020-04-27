package kafka.log;

import kafka.common.InvalidOffsetException;
import kafka.utils.Os;
import org.apache.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static kafka.utils.Preconditions.*;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex {

    private static Logger logger = Logger.getLogger(OffsetIndex.class);

    File file;
    long baseOffset;
    int maxIndexSize = -1;

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;

        boolean newlyCreated = file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        try {
            /* pre-allocate the file if necessary */
            if(newlyCreated) {
                if(maxIndexSize < 8)
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                raf.setLength(roundToExactMultiple(maxIndexSize, 8));
            }

            long len = raf.length();
            if(len < 0 || len % 8 != 0)
                throw new IllegalStateException("Index file " + file.getName() + " is corrupt, found " + len +
                        " bytes which is not positive or not a multiple of 8.");

            /* memory-map the file */
            MappedByteBuffer idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);

            /* set the position in the index for the next entry */
            if(newlyCreated)
                idx.position(0);
            else
                // if this is a pre-existing index, assume it is all valid and set position to last entry
                idx.position(roundToExactMultiple(idx.limit(), 8));
           this.mmap = idx;
        } finally {
            raf.close();
        }

        this.size = new AtomicInteger(mmap.position() / 8);
        this.maxEntries = mmap.limit() / 8;
        this.lastOffset = readLastOffset();
    }
    private Lock lock = new ReentrantLock();

    /* the memory mapping */
    private MappedByteBuffer mmap;

    /* the number of entries in the index */
    private AtomicInteger size ;

    /**
     * The maximum number of eight-byte entries this index can hold
     */
    volatile int maxEntries ;

    /* the last offset in the index */
    long lastOffset ;

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundToExactMultiple(67, 8) == 64
     */
    private int roundToExactMultiple(int number, int factor) {
       return factor * (number / factor);
    }

    /**
     * The last offset written to the index
     */
    private long readLastOffset() {
        long offset = 0;
        lock.lock();
        try{
            if(size.get() == 0){
                offset = 0;
            }else{
                offset = relativeOffset(this.mmap, size.get() - 1);
            }
        }finally {
            lock.unlock();
        }
        return baseOffset + offset;
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and it's corresponding physical file position.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(long targetOffset) {
        lock.lock();
        try{
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, targetOffset);
            if(slot == -1)
               return new OffsetPosition(baseOffset, 0);
            else
                return new OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot));
        }finally {
            lock.unlock();
        }
    }

    /**
     * Find the slot in which the largest offset less than or equal to the given
     * target offset is stored.
     * Return -1 if the least entry in the index is larger than the target offset or the index is empty
     */
    private int indexSlotFor(ByteBuffer idx,long targetOffset){
        // we only store the difference from the base offset so calculate that
        long relOffset = targetOffset - baseOffset;

        // check if the index is empty
        if(entries() == 0)
            return -1;

        // check if the target offset is smaller than the least offset
        if(relativeOffset(idx, 0) > relOffset)
            return -1;

        // binary search for the entry
        int lo = 0;
        int hi = entries()-1;
        while(lo < hi) {
            int mid = (int)Math.ceil(hi/2.0 + lo/2.0);
            int found = relativeOffset(idx, mid);
            if(found == relOffset)
                return mid;
            else if(found < relOffset)
                lo = mid;
            else
                hi = mid - 1;
        }
       return lo;
    }


    /* return the nth offset relative to the base offset */
    private int relativeOffset(ByteBuffer buffer,int n){
        return buffer.getInt(n * 8);
    }

    /* return the nth physical offset */
    private int physical(ByteBuffer buffer,int n){
        return buffer.getInt(n * 8 + 4);
    }
    /**
     * Get the nth offset mapping from the index
     */
    public OffsetPosition entry(int n) {
        lock.lock();
        try{
            if(n >= entries())
                throw new IllegalArgumentException(String.format("Attempt to fetch the %dth entry from an index of size %d.",n, entries()));
            ByteBuffer idx = mmap.duplicate();
            return new OffsetPosition(relativeOffset(idx, n), physical(idx, n));
        }finally {
            lock.unlock();
        }
    }

    /**
     * Append entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     */
    public void append(long offset, int position) {
        lock.lock();
        try{
            checkArgument(!isFull(), "Attempt to append to a full index (size = " + size + ").");
            if (size.get() == 0 || offset > lastOffset) {
                logger.debug(String.format("Adding index entry %d => %d to %s.",offset, position, file.getName()));
                this.mmap.putInt((int)(offset - baseOffset));
                this.mmap.putInt(position);
                this.size.incrementAndGet();
                this.lastOffset = offset;
                checkArgument(entries() * 8 == mmap.position(), entries() + " entries but file position in index is " + mmap.position() + ".");
            } else {
                throw new InvalidOffsetException(String
                        .format("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s.",offset, entries(), lastOffset, file.getName()));
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * True iff there are no more slots available in this index
     */
    public boolean isFull(){
        return entries() >= this.maxEntries;
    }

    /**
     * Truncate the entire index
     */
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public void truncateTo(long offset) {
        lock.lock();
        try{
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, offset);

            /* There are 3 cases for choosing the new size
             * 1) if there is no entry in the index <= the offset, delete everything
             * 2) if there is an entry for this exact offset, delete it and everything larger than it
             * 3) if there is no entry for this offset, delete everything larger than the next smallest
             */
            int newEntries ;
            if(slot < 0)
                newEntries = 0;
            else if(relativeOffset(idx, slot) == offset - baseOffset)
                newEntries = slot;
            else
                newEntries = slot + 1;
            truncateToEntries(newEntries);
        }finally {
            lock.unlock();
        }
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(int entries) {
        lock.lock();
        try{
            this.size.set(entries);
            mmap.position(this.size.get() * 8);
            this.lastOffset = readLastOffset();
        }finally {
            lock.unlock();
        }
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() throws IOException{
        lock.lock();
        try{
            resize(entries() * 8);
        }finally {
            lock.unlock();
        }
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    public void resize(int newSize) throws IOException{
        lock.lock();
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "rws");
            int roundedNewSize = roundToExactMultiple(newSize, 8);
            int position = this.mmap.position();

            /* Windows won't let us modify the file length while the file is mmapped :-( */
            if(Os.isWindows())
                forceUnmap(this.mmap);
            try {
                raf.setLength(roundedNewSize);
                this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                this.maxEntries = this.mmap.limit() / 8;
                this.mmap.position(position);
            } finally {
                raf.close();
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * Forcefully free the buffer's mmap. We do this only on windows.
     */
    private void forceUnmap(MappedByteBuffer m) {
        try {
            if(m instanceof sun.nio.ch.DirectBuffer)
               ((DirectBuffer) m).cleaner().clean();
        } catch (Throwable t){
            logger.warn("Error when freeing index buffer", t);
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        lock.lock();
        try{
            mmap.force();
        }finally {
            lock.unlock();
        }
    }

    /**
     * Delete this index file
     */
    public boolean delete() {
        logger.info("Deleting index " + this.file.getAbsolutePath());
        return this.file.delete();
    }

    /** Close the index */
    public void close() throws IOException{
        trimToValidSize();
    }

    public int entries() {
        return size.get();
    }

}

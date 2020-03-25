package kafka.consumer;

import kafka.cluster.Partition;
import kafka.common.ErrorMapping;
import kafka.message.ByteBufferMessageSet;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sun.org.apache.xml.internal.serializer.utils.Utils.messages;
import static sun.security.jgss.GSSToken.debug;

public class PartitionTopicInfo {

    private static Logger logger = Logger.getLogger(PartitionTopicInfo.class);

    String topic;
    int brokerId;
    Partition partition;
    private BlockingQueue<FetchedDataChunk> chunkQueue;
    private AtomicLong consumedOffset;
    private AtomicLong fetchedOffset;
    private AtomicInteger fetchSize;

    public PartitionTopicInfo(String topic, int brokerId, Partition partition, BlockingQueue<FetchedDataChunk> chunkQueue, AtomicLong consumedOffset, AtomicLong fetchedOffset, AtomicInteger fetchSize) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partition = partition;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;

        logger.debug("initial consumer offset of " + this + " is " + consumedOffset.get());
        logger.debug("initial fetch offset of " + this + " is " + fetchedOffset.get());
    }

    public long getConsumeOffset() {
        return consumedOffset.get();
    }

    public long getFetchOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(long newConsumeOffset)  {
        consumedOffset.set(newConsumeOffset);
        logger.debug("reset consume offset of " + this + " to " + newConsumeOffset);
    }

    public void resetFetchOffset(long newFetchOffset)  {
        fetchedOffset.set(newFetchOffset);
        logger.debug("reset fetch offset of ( %s ) to %d".format(this.toString(), newFetchOffset));
    }

    /**
     * Enqueue a message set for processing
     * @return the number of valid bytes
     */
    public long enqueue(ByteBufferMessageSet messages, long fetchOffset)  throws InterruptedException{
        long size = messages.validBytes();
        if(size > 0) {
            // update fetched offset to the compressed data chunk size, not the decompressed message set size
            logger.trace("Updating fetch offset = " + fetchedOffset.get() + " with size = " + size);
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
            long newOffset = fetchedOffset.addAndGet(size);
            logger. debug("updated fetch offset of ( %s ) to %d".format(this.toString(), newOffset));
        }
       return size;
    }

    /**
     *  add an empty message with the exception to the queue so that client can see the error
     */
    public void enqueueError(Throwable e, long fetchOffset)  throws InterruptedException {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(ErrorMapping.EmptyByteBuffer, 0,
                ErrorMapping.codeFor(e.getClass().getName()));
        chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
    }

    public String toString(){
        return topic + ":" + partition.toString() + ": fetched offset = " + fetchedOffset.get() +
                ": consumed offset = " + consumedOffset.get();
    }

}

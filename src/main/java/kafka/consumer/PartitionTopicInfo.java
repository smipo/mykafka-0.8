package kafka.consumer;

import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class PartitionTopicInfo {

    public static long InvalidOffset = -1L;

    public static boolean isOffsetInvalid(long offset) {
      return  offset < 0L;
    }

    private static Logger logger = Logger.getLogger(PartitionTopicInfo.class);

    public String topic;
    public int partitionId;
    public BlockingQueue<FetchedDataChunk> chunkQueue;
    public AtomicLong consumedOffset;
    public AtomicLong fetchedOffset;
    public AtomicInteger fetchSize;
    public String clientId;

    public PartitionTopicInfo(String topic, int partitionId,  BlockingQueue<FetchedDataChunk> chunkQueue, AtomicLong consumedOffset, AtomicLong fetchedOffset, AtomicInteger fetchSize,String clientId) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;
        this.clientId = clientId;
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
    public long enqueue(ByteBufferMessageSet messages)  throws InterruptedException{
        long size = messages.validBytes();
        if(size > 0) {
            long next = 0;
            Iterator<MessageAndOffset> iterator = messages.shallowIterator();
            while (iterator.hasNext()){
                MessageAndOffset messageAndOffset =  iterator.next();
                next = messageAndOffset.nextOffset();
            }
            // update fetched offset to the compressed data chunk size, not the decompressed message set size
            logger.trace("Updating fetch offset = " + fetchedOffset.get() + " with size = " + size);
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get()));
            long newOffset = fetchedOffset.addAndGet(size);
            fetchedOffset.set(next);
            logger. debug("updated fetch offset of ( %s ) to %d".format(this.toString(), newOffset));
        }else if(messages.sizeInBytes() > 0) {
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get()));
        }
       return size;
    }


    @Override
    public String toString() {
        return "PartitionTopicInfo{" +
                "topic='" + topic + '\'' +
                ", chunkQueue=" + chunkQueue +
                ", consumedOffset=" + consumedOffset +
                ", fetchedOffset=" + fetchedOffset +
                ", fetchSize=" + fetchSize +
                ", clientId='" + clientId + '\'' +
                '}';
    }
}

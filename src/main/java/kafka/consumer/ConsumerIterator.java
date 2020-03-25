package kafka.consumer;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.utils.IteratorTemplate;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static kafka.consumer.ZookeeperConsumerConnector.shutdownCommand;

/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 *
 */
public class ConsumerIterator<T> extends IteratorTemplate<MessageAndMetadata<T>> {

    private static Logger logger = Logger.getLogger(ConsumerIterator.class);

    BlockingQueue<FetchedDataChunk> channel;
    int consumerTimeoutMs;
    private Decoder<T> decoder;
    boolean enableShallowIterator;

    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs, Decoder<T> decoder, boolean enableShallowIterator) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
        this.enableShallowIterator = enableShallowIterator;
    }

    private AtomicReference<Iterator<MessageAndOffset>> current = new AtomicReference<>();
    private PartitionTopicInfo currentTopicInfo = null;
    private long consumedOffset = -1L;


    public MessageAndMetadata<T>  next(){
        MessageAndMetadata<T>  item = super.next();
        if(consumedOffset < 0)
            throw new IllegalStateException("Offset returned by the message set is invalid %d".format(consumedOffset + ""));
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        String topic = currentTopicInfo.topic;
        logger.trace("Setting %s consumed offset to %d".format(topic, consumedOffset));
        return item;
    }

    protected MessageAndMetadata<T> makeNext() {
        FetchedDataChunk currentDataChunk = null;
        // if we don't have an iterator, get one
        Iterator<MessageAndOffset> localCurrent = current.get();
        if(localCurrent == null || !localCurrent.hasNext()) {
            try{
                if (consumerTimeoutMs < 0)
                    currentDataChunk = channel.take();
                else {
                    currentDataChunk = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
                    if (currentDataChunk == null) {
                        // reset state to make the iterator re-iterable
                        resetState();
                        throw new ConsumerTimeoutException();
                    }
                }
            }catch (InterruptedException e){
                logger.error("consumer makeNext Eeror:",e);
            }
            //todo
            if(currentDataChunk == null || currentDataChunk.fetchOffset == -1L || currentDataChunk.equals(ZookeeperConsumerConnector.shutdownCommand)) {
                logger.debug("Received the shutdown command");
                channel.offer(currentDataChunk);
                return allDone();
            } else {
                currentTopicInfo = currentDataChunk.topicInfo();
                if (currentTopicInfo.getConsumeOffset() != currentDataChunk.fetchOffset) {
                    logger.error("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data"
                            .format(currentTopicInfo.getConsumeOffset() + "", currentDataChunk.fetchOffset, currentTopicInfo));
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.fetchOffset);
                }
                if (enableShallowIterator) localCurrent =  currentDataChunk.messages.shallowIterator();
                else localCurrent = currentDataChunk.messages.iterator();
                current.set(localCurrent);
            }
        }
        MessageAndOffset item = localCurrent.next();
        consumedOffset = item.offset();

        return new MessageAndMetadata(decoder.toEvent(item.message()), currentTopicInfo.topic);
    }

    public void clearCurrentChunk() {
        logger.info("Clearing the current data chunk for this consumer iterator");
        current.set(null);
    }
}

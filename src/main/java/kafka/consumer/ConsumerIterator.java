package kafka.consumer;

import kafka.common.MessageSizeTooLargeException;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.utils.IteratorTemplate;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
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
public class ConsumerIterator<K, V> extends IteratorTemplate<MessageAndMetadata<K, V>> {

    private static Logger logger = Logger.getLogger(ConsumerIterator.class);

    public BlockingQueue<FetchedDataChunk> channel;
    public int consumerTimeoutMs;
    public Decoder<K> keyDecoder;
    public Decoder<V> valueDecoder;
    public String clientId;


    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder,  String clientId) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;
    }

    private AtomicReference<Iterator<MessageAndOffset>> current = new AtomicReference<>();
    private PartitionTopicInfo currentTopicInfo = null;
    private long consumedOffset = -1L;


    public MessageAndMetadata<K,V>  next(){
        MessageAndMetadata<K,V>  item = super.next();
        if(consumedOffset < 0)
            throw new IllegalStateException(String.format("Offset returned by the message set is invalid %d",consumedOffset));
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        String topic = currentTopicInfo.topic;
        logger.trace(String.format("Setting %s consumed offset to %d",topic, consumedOffset));
        return item;
    }

    protected MessageAndMetadata<K,V> makeNext() {
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
            if(currentDataChunk == ZookeeperConsumerConnector.shutdownCommand) {
                logger.debug("Received the shutdown command");
                channel.offer(currentDataChunk);
                return allDone();
            } else {
                currentTopicInfo = currentDataChunk.topicInfo();
                if (currentTopicInfo.getConsumeOffset() < currentDataChunk.fetchOffset) {
                    logger.error(String
                            .format("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data",currentTopicInfo.getConsumeOffset(), currentDataChunk.fetchOffset, currentTopicInfo));
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.fetchOffset);
                }
                localCurrent = currentDataChunk.messages.iterator();
                current.set(localCurrent);
            }
            // if we just updated the current chunk and it is empty that means the fetch size is too small!
            if(currentDataChunk.messages.validBytes() == 0)
                throw new MessageSizeTooLargeException(String
                                .format("Found a message larger than the maximum fetch size of this consumer on topic " +
                                        "%s partition %d at fetch offset %d. Increase the fetch size, or decrease the maximum message size the broker will allow.",currentDataChunk.topicInfo.topic, currentDataChunk.topicInfo.partitionId, currentDataChunk.fetchOffset));

        }
        MessageAndOffset item = localCurrent.next();
        // reject the messages that have already been consumed
        while (item.offset() < currentTopicInfo.getConsumeOffset() && localCurrent.hasNext()) {
            item = localCurrent.next();
        }
        consumedOffset = item.nextOffset();

        item.message().ensureValid(); // validate checksum of message to ensure it is valid

        ByteBuffer keyBuffer = item.message().key();
        return new MessageAndMetadata(keyBuffer == null?null:keyDecoder.fromBytes(Utils.readBytes(keyBuffer)), valueDecoder.fromBytes(Utils.readBytes(item.message().payload())), currentTopicInfo.topic, currentTopicInfo.partitionId, item.offset());
    }

    public void clearCurrentChunk() {
        logger.info("Clearing the current data chunk for this consumer iterator");
        current.set(null);
    }
}

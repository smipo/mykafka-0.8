package kafka.consumer;

import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import java.util.concurrent.BlockingQueue;

public class KafkaStream<T> implements Iterable<MessageAndMetadata<T>>{

    BlockingQueue<FetchedDataChunk> queue;
    int consumerTimeoutMs;
    private Decoder<T> decoder;
    boolean enableShallowIterator;

    private ConsumerIterator<T> iter ;

    public KafkaStream(BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<T> decoder, boolean enableShallowIterator) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
        this.enableShallowIterator = enableShallowIterator;

        this.iter =  new ConsumerIterator<T>(queue, consumerTimeoutMs, decoder, enableShallowIterator);
    }

    /**
     *  Create an iterator over messages in the stream.
     */
   public ConsumerIterator<T> iterator() {
        return iter;
    }

    /**
     * This method clears the queue being iterated during the consumer rebalancing. This is mainly
     * to reduce the number of duplicates received by the consumer
     */
    public void clear() {
        iter.clearCurrentChunk();
    }


}

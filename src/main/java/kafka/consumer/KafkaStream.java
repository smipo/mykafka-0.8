package kafka.consumer;

import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import java.util.concurrent.BlockingQueue;

public class KafkaStream<K,V> implements Iterable<MessageAndMetadata<K,V>>{

    public  BlockingQueue<FetchedDataChunk> queue;
    public int consumerTimeoutMs;
    public Decoder<K> keyDecoder;
    public Decoder<V> valueDecoder;
    public String clientId;

    private ConsumerIterator<K,V> iter ;

    public KafkaStream(BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder, String clientId) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;

        this.iter =  new ConsumerIterator<K,V>(queue, consumerTimeoutMs, keyDecoder,valueDecoder, clientId);
    }

    /**
     *  Create an iterator over messages in the stream.
     */
   public ConsumerIterator<K,V> iterator() {
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

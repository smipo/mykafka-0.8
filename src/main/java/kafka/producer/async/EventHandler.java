package kafka.producer.async;

import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Handler that dispatches the batched data from the queue.
 */
public interface EventHandler< K,V> {


    /**
     * Callback to dispatch the batched data and send it to a Kafka server
     * @param events the data sent to the producer
     */
    public void handle(List<KeyedMessage<K,V>> events)  throws Exception;

    /**
     * Cleans up and shuts down the event handler
     */
    public void close();
}

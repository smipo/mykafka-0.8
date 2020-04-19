package kafka.javaapi.producer;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;

public class Producer<K,V> {

     kafka.producer.Producer<K,V> underlying;

    public Producer(kafka.producer.Producer<K, V> underlying) {
        this.underlying = underlying;
    }

    public Producer(ProducerConfig config) {
        this(new kafka.producer.Producer<K, V>(config));
    }
    /**
     * Sends the data to a single topic, partitioned by key, using either the
     * synchronous or the asynchronous producer
     * @param message the producer data object that encapsulates the topic, key and message data
     */
    public void send(KeyedMessage<K,V> message) throws Exception {
        List<KeyedMessage<K,V>> messages = new ArrayList<>();
        messages.add(message);
        underlying.send(messages);
    }

    /**
     * Use this API to send data to multiple topics
     * @param messages list of producer data objects that encapsulate the topic, key and message data
     */
    public void send(List<KeyedMessage<K,V>> messages) throws Exception {
        underlying.send(messages);
    }

    /**
     * Close API to close the producer pool connections to all Kafka brokers. Also closes
     * the zookeeper client connection if one exists
     */
    public void close() throws InterruptedException {
        underlying.close();
    }
}

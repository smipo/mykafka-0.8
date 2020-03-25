package kafka.producer.async;

import kafka.producer.SyncProducer;
import kafka.serializer.Encoder;

import java.util.Properties;

public interface EventHandler<T> {

    /**
     * Initializes the event handler using a Properties object
     * @param props the properties used to initialize the event handler
     */
    public void init(Properties props) ;

    /**
     * Callback to dispatch the batched data and send it to a Kafka server
     * @param events the data sent to the producer
     * @param producer the low-level producer used to send the data
     */
    public void handle(QueueItem<T>[] events, SyncProducer producer, Encoder<T> encoder)  throws Exception;

    /**
     * Cleans up and shuts down the event handler
     */
    public void close();
}

package kafka.producer.async;

import kafka.producer.SyncProducerConfig;
import kafka.utils.Utils;

import java.util.Properties;

public class AsyncProducerConfig extends SyncProducerConfig {

    public AsyncProducerConfig(Properties props){
        super(props);

        queueTime = Utils.getInt(props, "queue.time", 5000);
        queueSize = Utils.getInt(props, "queue.size", 10000);
        enqueueTimeoutMs = Utils.getInt(props, "queue.enqueueTimeout.ms", 0);
        batchSize = Utils.getInt(props, "batch.size", 200);
        serializerClass = Utils.getString(props, "serializer.class", "kafka.serializer.DefaultEncoder");
        cbkHandler = Utils.getString(props, "callback.handler", null);
        cbkHandlerProps = Utils.getProps(props, "callback.handler.props", null);
        eventHandler = Utils.getString(props, "event.handler", null);
        eventHandlerProps = Utils.getProps(props, "event.handler.props", null);
    }

    /* maximum time, in milliseconds, for buffering data on the producer queue */
    public int queueTime ;

    /** the maximum size of the blocking queue for buffering on the producer */
    public int queueSize ;

    /**
     * Timeout for event enqueue:
     * 0: events will be enqueued immediately or dropped if the queue is full
     * -ve: enqueue will block indefinitely if the queue is full
     * +ve: enqueue will block up to this many milliseconds if the queue is full
     */
    public  int enqueueTimeoutMs;

    /** the number of messages batched at the producer */
    public int batchSize;

    /** the serializer class for events */
    public String serializerClass ;

    /** the callback handler for one or multiple events */
    public String cbkHandler ;

    /** properties required to initialize the callback handler */
    public Properties cbkHandlerProps ;

    /** the handler for events */
    public String eventHandler ;

    /** properties required to initialize the callback handler */
    public Properties eventHandlerProps;
}

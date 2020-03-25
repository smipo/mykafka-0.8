package kafka.producer.async;

import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducer;
import kafka.serializer.Encoder;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncProducer<T> {

    private static Logger logger = Logger.getLogger(AsyncProducer.class);

    public static Object Shutdown = new Object();
    public static Random random = new Random();
    public static String ProducerMBeanName = "kafka.producer.Producer:type=AsyncProducerStats";
    public static String  ProducerQueueSizeMBeanName = "kafka.producer.Producer:type=AsyncProducerQueueSizeStats";

    AsyncProducerConfig config;
    SyncProducer producer;
    Encoder<T> serializer;
    EventHandler<T> eventHandler = null;
    Properties eventHandlerProps = null;
    CallbackHandler<T> cbkHandler = null;
    Properties cbkHandlerProps = null;

    public AsyncProducer(AsyncProducerConfig config, SyncProducer producer, Encoder<T> serializer, EventHandler<T> eventHandler, Properties eventHandlerProps, CallbackHandler<T> cbkHandler, Properties cbkHandlerProps) {
       init(config,  producer,  serializer,  eventHandler,  eventHandlerProps,  cbkHandler,  cbkHandlerProps);
    }
    public AsyncProducer(AsyncProducerConfig config) throws ClassNotFoundException,InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        Encoder<T> encoder = Utils.getObject(config.serializerClass) == null?null:(Encoder)Utils.getObject(config.serializerClass);
        EventHandler<T> eventHandler = Utils.getObject(config.eventHandler) == null?null:(EventHandler)Utils.getObject(config.eventHandler);
        CallbackHandler callbackHandler = Utils.getObject(config.cbkHandler) == null?null:(CallbackHandler)Utils.getObject(config.cbkHandler);
        init(config,
                new SyncProducer(config),
                encoder,
                eventHandler,
                config.eventHandlerProps,
                callbackHandler,
                config.cbkHandlerProps);
    }
    private  void init(AsyncProducerConfig config, SyncProducer producer, Encoder<T> serializer, EventHandler<T> eventHandler, Properties eventHandlerProps, CallbackHandler<T> cbkHandler, Properties cbkHandlerProps){
        this.config = config;
        this.producer = producer;
        this.serializer = serializer;
        this.eventHandler = eventHandler;
        this.eventHandlerProps = eventHandlerProps;
        this.cbkHandler = cbkHandler;
        this.cbkHandlerProps = cbkHandlerProps;
        queue = new LinkedBlockingQueue<>(config.queueSize);
        // initialize the callback handlers
        if(this.eventHandler != null)
            this.eventHandler.init(this.eventHandlerProps);
        if(this.cbkHandler != null)
            this.cbkHandler.init(this.cbkHandlerProps);
        if(this.eventHandler == null) this.eventHandler = new DefaultEventHandler<T>(new ProducerConfig(config.props), this.cbkHandler);
        sendThread = new ProducerSendThread("ProducerSendThread-" + asyncProducerID, queue,
                serializer, producer,this.eventHandler,
                this.cbkHandler, config.queueTime, config.batchSize, AsyncProducer.Shutdown);
        sendThread.setDaemon(false);
    }
    private AtomicBoolean closed = new AtomicBoolean(false);
    private LinkedBlockingQueue<QueueItem<T>> queue ;
    private int asyncProducerID = AsyncProducer.random.nextInt();
    private ProducerSendThread sendThread ;

    public void start(){
        sendThread.start();
    }
    public void send(String topic, T event, int partition) {
        if(closed.get())
            throw new QueueClosedException("Attempt to add event to a closed queue.");

        QueueItem<T> data = new QueueItem<T>(event, topic, partition);
        if(cbkHandler != null)
            data = cbkHandler.beforeEnqueue(data);
        boolean added ;
        if(config.enqueueTimeoutMs == 0){
            added = queue.offer(data);
        }else{
            try {
                if(config.enqueueTimeoutMs < 0 ){
                    queue.put(data);
                    added =  true;
                }else{
                    added = queue.offer(data, config.enqueueTimeoutMs, TimeUnit.MILLISECONDS);
                }
            }
            catch (InterruptedException e){
                String msg = "%s interrupted during enqueue of event %s.".format(
                        getClass().getSimpleName(), event.toString());
                logger.error(msg);
                throw new AsyncProducerInterruptedException(msg);
            }
        }

        if(cbkHandler != null)
            cbkHandler.afterEnqueue(data, added);

        if(!added) {
            logger.error("Event queue is full of unsent messages, could not send event: " + event.toString());
            throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event.toString());
        }else {
            if(logger.isTraceEnabled()) {
                logger.trace("Added event to send queue for topic: " + topic + ", partition: " + partition + ":" + event.toString());
                logger.trace("Remaining queue size: " + queue.remainingCapacity());
            }
        }
    }

    public void close() throws InterruptedException{
        if(cbkHandler != null) {
            cbkHandler.close();
            logger.info("Closed the callback handler");
        }
        closed.set(true);
        queue.put(new QueueItem(AsyncProducer.Shutdown, null, -1));
        if(logger.isDebugEnabled())
            logger.debug("Added shutdown command to the queue");
        sendThread.shutdown();
        sendThread.awaitShutdown();
        producer.close();
        logger.info("Closed AsyncProducer");
    }

}

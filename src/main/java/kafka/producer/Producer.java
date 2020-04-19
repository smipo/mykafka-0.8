package kafka.producer;


import kafka.api.TopicMetadata;
import kafka.common.ProducerClosedException;
import kafka.common.QueueFullException;
import kafka.producer.async.DefaultEventHandler;
import kafka.producer.async.EventHandler;
import kafka.producer.async.ProducerSendThread;
import kafka.serializer.DefaultEncoder;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producer<K,V> {

    private static Logger logger = Logger.getLogger(Producer.class);

    ProducerConfig config;
    EventHandler<K,V> eventHandler;

    public Producer(ProducerConfig config, EventHandler<K, V> eventHandler) {
        this.config = config;
        this.eventHandler = eventHandler;
        queue = new LinkedBlockingQueue<>(config.queueBufferingMaxMessages);
        if(config.producerType.equals("async")){
            sync = false;
            producerSendThread = new ProducerSendThread<K,V>("ProducerSendThread-" + config.clientId,
                    queue,
                    eventHandler,
                    config.queueBufferingMaxMs,
                    config.batchNumMessages,
                    config.clientId);
            producerSendThread.start();
        }
    }

    public Producer(ProducerConfig config){
      this(config,new DefaultEventHandler(config, new DefaultPartitioner<K>(), new DefaultEncoder(), new DefaultEncoder(),new ProducerPool(config), new HashMap<>()));
    }

    AtomicBoolean hasShutdown = new AtomicBoolean(false);
    private LinkedBlockingQueue<KeyedMessage<K,V>> queue;

    private boolean sync = true;
    private ProducerSendThread<K,V> producerSendThread = null;
    private Object lock = new Object();


    /**
     * Sends the data, partitioned by key to the topic using either the
     * synchronous or the asynchronous producer
     * @param messages the producer data object that encapsulates the topic, key and message data
     */
    public void send(List<KeyedMessage<K,V>> messages) throws Exception {
         synchronized(lock) {
            if (hasShutdown.get())
                throw new ProducerClosedException();
            if(sync){
                eventHandler.handle(messages);
            }else{
                asyncSend(messages);
            }
        }
    }


    private void asyncSend(List<KeyedMessage<K,V>> messages) {
        for (KeyedMessage<K,V> message : messages) {
            boolean added = false;
            if(config.queueEnqueueTimeoutMs == 0){
                queue.offer(message);
            }else{
                try {
                    if(config.queueEnqueueTimeoutMs < 0){
                        queue.put(message);
                        added = true;
                    }else{
                        added = queue.offer(message, config.queueEnqueueTimeoutMs, TimeUnit.MILLISECONDS);
                    }
                }
                catch(InterruptedException e) {
                    added =  false;
                }
            }
            if(!added) {
                throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString());
            }else {
                logger.trace("Added to send queue an event: " + message.toString());
                logger.trace("Remaining queue size: " + queue.remainingCapacity());
            }
        }
    }

    /**
     * Close API to close the producer pool connections to all Kafka brokers. Also closes
     * the zookeeper client connection if one exists
     */
    public void close() throws InterruptedException {
         synchronized(lock) {
            boolean canShutdown = hasShutdown.compareAndSet(false, true);
            if(canShutdown) {
                logger.info("Shutting down producer");
                if (producerSendThread != null)
                    producerSendThread.shutdown();
                eventHandler.close();
            }
        }
    }
}

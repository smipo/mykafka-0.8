package kafka.producer.async;

import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ProducerSendThread<K,V> extends Thread{

    private static Logger logger = Logger.getLogger(ProducerSendThread.class);
    String threadName;
    BlockingQueue<KeyedMessage<K,V>> queue;
    EventHandler<K,V> handler;
    long queueTime;
    int batchSize;
    String clientId;

    public ProducerSendThread( String threadName, BlockingQueue<KeyedMessage<K, V>> queue, EventHandler<K, V> handler, long queueTime, int batchSize, String clientId) {
        super(threadName);
        this.threadName = threadName;
        this.queue = queue;
        this.handler = handler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
        this.clientId = clientId;
    }

    CountDownLatch shutdownLatch = new CountDownLatch(1);
    private KeyedMessage<K,V> shutdownCommand = new KeyedMessage<K,V> ("shutdown", null, null);

    @Override
    public void run() {
        try {
            processEvents();
        }catch(Throwable e) {
           logger.error("Error in sending events: ", e);
        }finally {
            shutdownLatch.countDown();
        }
    }

    public void shutdown () throws InterruptedException {
            logger.info("Begin shutting down ProducerSendThread");
            queue.put(shutdownCommand);
            shutdownLatch.await();
            logger.info("Shutdown ProducerSendThread complete");
    }

    private void processEvents() throws InterruptedException {
        long lastSend = System.currentTimeMillis();
        List<KeyedMessage<K,V>> events = new ArrayList<>();
        boolean full = false;

        // drain the queue until you get a shutdown command
        while (true){
            KeyedMessage<K,V> currentQueueItem = queue.poll(Math.max(0, (lastSend + queueTime) - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            if(currentQueueItem.topic.equals("shutdown")){
                break;
            }
            long elapsed = (System.currentTimeMillis() - lastSend);
            // check if the queue time is reached. This happens when the poll method above returns after a timeout and
            // returns a null object
            boolean expired = currentQueueItem == null;
            if(currentQueueItem != null) {
                logger.trace("Dequeued item for topic %s, partition key: %s, data: %s"
                        .format(currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message));
                events.add(currentQueueItem);
            }

            // check if the batch size is reached
            full = events.size() >= batchSize;

            if(full || expired) {
                if(expired)
                    logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                if(full)
                    logger.debug("Batch full. Sending..");
                // if either queue time has reached or batch size has reached, dispatch to event handler
                tryToHandle(events);
                lastSend = System.currentTimeMillis();
                events = new ArrayList<>();
            }
        }
        // send the last batch of events
        tryToHandle(events);
        if(queue.size() > 0)
            throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
                    .format(queue.size()+""));
    }

    public void tryToHandle(List<KeyedMessage<K,V>> events) {
        int size = events.size();
        try {
            logger.debug("Handling " + size + " events");
            if(size > 0)
                handler.handle(events);
        }catch(Throwable e) {
            logger.error("Error in handling batch of " + size + " events", e);
        }
    }

}

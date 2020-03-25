package kafka.producer.async;

import kafka.producer.SyncProducer;
import kafka.serializer.Encoder;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


public class ProducerSendThread<T> extends Thread{

    private static Logger logger = Logger.getLogger(ProducerSendThread.class);

    String threadName;
    BlockingQueue<QueueItem<T>> queue;
    Encoder<T> serializer;
    SyncProducer underlyingProducer;
    EventHandler<T> handler;
    CallbackHandler<T> cbkHandler;
    long queueTime;
    int batchSize;
    Object shutdownCommand;

    public ProducerSendThread(String threadName, BlockingQueue<QueueItem<T>> queue, Encoder<T> serializer, SyncProducer underlyingProducer, EventHandler<T> handler, CallbackHandler<T> cbkHandler, long queueTime, int batchSize, Object shutdownCommand) {
        super(threadName);
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = underlyingProducer;
        this.handler = handler;
        this.cbkHandler = cbkHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
        this.shutdownCommand = shutdownCommand;
    }

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile boolean stopped = false;

    public  void awaitShutdown()throws InterruptedException {
        shutdownLatch.await();
    }

    public void shutdown() {
        handler.close();
        logger.info("Shutdown thread complete");
    }
    public void run() {

        try {
            QueueItem<T>[] remainingEvents = processEvents();
            logger.debug("Remaining events = " + remainingEvents.length);

            // handle remaining events
            if(remainingEvents.length > 0) {
                logger.debug("Dispatching last batch of %d events to the event handler".format(remainingEvents.length + ""));
                tryToHandle(remainingEvents);
            }
        }catch (Exception e){
            logger.error("Error in sending events: ", e);
        }finally {
            shutdownLatch.countDown();
        }
    }
    private QueueItem<T>[] processEvents() throws InterruptedException{
        long lastSend = System.currentTimeMillis();
        List<QueueItem<T>> events = new ArrayList<>();
        boolean full = false;
        // drain the queue until you get a shutdown command
        while (!stopped){
            QueueItem<T> item = queue.poll(Math.max(0, (lastSend + queueTime) - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - lastSend;
            boolean expired = item == null;
            if(item != null)
                logger.trace("Dequeued item for topic %s and partition %d"
                        .format(item.getTopic(), item.getPartition()));
            // handle the dequeued current item
            if(cbkHandler != null)
                events.addAll(cbkHandler.afterDequeuingExistingData(item));
            else {
                if (item != null)
                    events.add(item);
            }
            // check if the batch size is reached
            full = events.size() >= batchSize;

            if(full || expired) {
                if(expired) logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                if(full) logger.debug("Batch full. Sending..");
                // if either queue time has reached or batch size has reached, dispatch to event handler
                tryToHandle(conversion(events));
                lastSend = System.currentTimeMillis();
                events = new ArrayList<>();
            }
        }
       return conversion(events);
    }
    public void tryToHandle(QueueItem<T>[] events) {
        try {
            logger.debug("Handling " + events.length + " events");
            if(events.length > 0)
                handler.handle(events, underlyingProducer, serializer);
        }catch(Exception e) {
            logger.error("Error in handling batch of " + events.length + " events", e);
        }
    }

    private void logEvents(String tag,QueueItem<T>[] events) {
        if(logger.isTraceEnabled()) {
            logger.trace("events for " + tag + ":");
            for (QueueItem<T> event : events)
                logger.trace(event.getData().toString());
        }
    }
    private QueueItem<T>[] conversion(List<QueueItem<T>> events){
        QueueItem<T>[] items = new QueueItem[events.size()];
        for(int i = 0;i < events.size();i++){
            items[i] = events.get(i);
        }
        return items;
    }
}

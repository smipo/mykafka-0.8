package kafka.utils;

import org.apache.log4j.Logger;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaScheduler {

    private static Logger logger = Logger.getLogger(KafkaScheduler.class);

    int numThreads;
    String baseThreadName;
    boolean isDaemon;

    private AtomicLong threadId = new AtomicLong(0);
    private ScheduledThreadPoolExecutor executor = null;

    public KafkaScheduler( int numThreads,
            String baseThreadName,
            boolean isDaemon){
        this.numThreads = numThreads;
        this.baseThreadName = baseThreadName;
        this.isDaemon = isDaemon;

        this.executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {
           public Thread newThread(Runnable r){
               Thread t = new Thread(r, baseThreadName + threadId.getAndIncrement());
                t.setDaemon(isDaemon);
                return t;
            }
        });
        this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
    public ScheduledFuture scheduleWithRate(Runnable command, long delayMs, long periodMs) {
       return executor.scheduleAtFixedRate(command, delayMs, periodMs, TimeUnit.MILLISECONDS);
    }
    public void shutdownNow() {
        executor.shutdownNow();
        logger.info("force shutdown scheduler " + baseThreadName);
    }

    public void shutdown() {
        executor.shutdown();
        logger.info("shutdown scheduler " + baseThreadName);
    }

}

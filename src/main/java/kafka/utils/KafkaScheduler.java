package kafka.utils;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaScheduler {

    private static Logger logger = Logger.getLogger(KafkaScheduler.class);

    int numThreads;

    public KafkaScheduler(int numThreads){
        this.numThreads = numThreads;

    }
    private ScheduledThreadPoolExecutor executor = null;
    private ThreadFactory daemonThreadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r){
            return Utils.newThread(r, true);
        }
    };
    private ThreadFactory nonDaemonThreadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r){
            return Utils.newThread(r, false);
        }
    };
    private Map<String, AtomicInteger> threadNamesAndIds = new HashMap<>();


    public void startup()  {
        executor = new ScheduledThreadPoolExecutor(numThreads);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public boolean hasShutdown(){
        return executor.isShutdown();
    }

    private void ensureExecutorHasStarted() {
        if(executor == null)
            throw new IllegalStateException("Kafka scheduler has not been started");
    }

    public void scheduleWithRate(Runnable runnable, long delayMs,long periodMs, boolean isDaemon){
        ensureExecutorHasStarted();
        if(isDaemon)
            executor.setThreadFactory(daemonThreadFactory);
        else
            executor.setThreadFactory(nonDaemonThreadFactory);
        executor.scheduleAtFixedRate(runnable, delayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public String currentThreadName(String name){
        AtomicInteger threadId =  threadNamesAndIds.putIfAbsent(name, new AtomicInteger(0));
        if(threadId == null){
            threadId = new AtomicInteger(0);
        }
        return name + threadId.incrementAndGet();
    }
    public void shutdownNow() {
        ensureExecutorHasStarted();
        executor.shutdownNow();
        logger.info("Forcing shutdown of Kafka scheduler");
    }

    public void shutdown() {
        ensureExecutorHasStarted();
        executor.shutdown();
        logger.info("Shutdown Kafka scheduler");
    }

}

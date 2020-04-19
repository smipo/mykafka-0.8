package kafka.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread extends Thread{

    private static Logger logger = Logger.getLogger(ShutdownableThread.class);

    public boolean isInterruptible;

    public ShutdownableThread(String name,boolean isInterruptible){
        super(name);
        this.setDaemon(false);
        this.isInterruptible = isInterruptible;
    }
    public AtomicBoolean isRunning = new AtomicBoolean(true);

    public CountDownLatch shutdownLatch = new CountDownLatch(1);

    public void shutdown() throws InterruptedException, IOException {
        logger.info("Shutting down");
        isRunning.set(false);
        if (isInterruptible)
            interrupt();
        shutdownLatch.await();
        logger.info("Shutdown completed");
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() throws InterruptedException{
        shutdownLatch.await();
    }

    public abstract void doWork() throws Throwable;

    @Override
    public void run() {
        logger.info("Starting ");
        try{
            while(isRunning.get()){
                doWork();
            }
        } catch(Throwable e){
            if(isRunning.get())
                logger.error("Error due to ", e);
        }
        shutdownLatch.countDown();
        logger.info("Stopped ");
    }

}

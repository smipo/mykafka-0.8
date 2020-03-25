package kafka.server;

import kafka.log.LogManager;
import kafka.network.SocketServer;
import kafka.utils.KafkaScheduler;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaServer {

    private static Logger logger = Logger.getLogger(KafkaServer.class);

    KafkaConfig config;

    public KafkaServer(KafkaConfig config){
        this.config = config;
    }

    String CLEAN_SHUTDOWN_FILE = ".kafka_cleanshutdown";
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private String statsMBeanName = "kafka:type=kafka.SocketServerStats";

    SocketServer socketServer = null;

    KafkaScheduler scheduler = new KafkaScheduler(1, "kafka-logcleaner-", false);

    private LogManager logManager = null;


    /**
     * Start up API for bringing up a single instance of the Kafka server.
     * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
     */
    public void startup() throws InterruptedException, IOException{
        logger.info("Starting Kafka server...");
        isShuttingDown = new AtomicBoolean(false);
        shutdownLatch = new CountDownLatch(1);
        boolean needRecovery = true;
        File cleanShutDownFile = new File(new File(config.logDir), CLEAN_SHUTDOWN_FILE);
        if (cleanShutDownFile.exists()) {
            needRecovery = false;
            cleanShutDownFile.delete();
        }
        logManager = new LogManager(config,
                scheduler,
                System.currentTimeMillis(),
                1000L * 60 * 60 * config.logRollHours,
                1000L * 60 * config.logCleanupIntervalMinutes,
                1000L * 60 * 60 * config.logRetentionHours,
                needRecovery);

        KafkaRequestHandlers handlers = new KafkaRequestHandlers(logManager);
        socketServer = new SocketServer(config.port,
                config.numThreads,
                config.monitoringPeriodSecs,
                handlers,
                config.socketSendBuffer,
                config.socketReceiveBuffer,
                config.maxSocketRequestSize);

        socketServer.startup();
        /**
         *  Registers this broker in ZK. After this, consumers can connect to broker.
         *  So this should happen after socket server start.
         */
        logManager.startup();
        logger.info("Kafka server started.");
    }

    /**
     * Shutdown API for shutting down a single instance of the Kafka server.
     * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
     */
    public void shutdown() throws InterruptedException, IOException {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (canShutdown) {
            logger.info("Shutting down Kafka server");
            scheduler.shutdown();
            if (socketServer != null)
                socketServer.shutdown();
            if (logManager != null)
                logManager.close();

            File cleanShutDownFile = new File(new File(config.logDir), CLEAN_SHUTDOWN_FILE);
            cleanShutDownFile.createNewFile();

            shutdownLatch.countDown();
            logger.info("Kafka server shut down completed");
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    public LogManager getLogManager() {
        return logManager;
    }

}

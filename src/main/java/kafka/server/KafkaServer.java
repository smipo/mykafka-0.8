package kafka.server;

import kafka.api.ControlledShutdownRequest;
import kafka.api.ControlledShutdownResponse;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.controller.KafkaController;
import kafka.log.LogManager;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.network.SocketServer;
import kafka.utils.KafkaScheduler;
import kafka.utils.ZkUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
public class KafkaServer {

    private static Logger logger = Logger.getLogger(KafkaServer.class);

    public KafkaConfig config;
    public long millis;

    public KafkaServer(KafkaConfig config,long millis) {
        this.config = config;
        this.millis = millis;
    }

    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean startupComplete = new AtomicBoolean(false);
    AtomicInteger correlationId  = new AtomicInteger(0);
    SocketServer socketServer  = null;
    KafkaRequestHandler.KafkaRequestHandlerPool requestHandlerPool = null;
    LogManager logManager = null;
    KafkaZooKeeper kafkaZookeeper = null;
    ReplicaManager replicaManager = null;
    KafkaApis apis = null;
    KafkaController kafkaController = null;
    KafkaScheduler kafkaScheduler = new KafkaScheduler(4);


    /**
     * Start up API for bringing up a single instance of the Kafka server.
     * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
     */
    public void startup() throws IOException, InterruptedException {
        logger.info("Starting");
        isShuttingDown = new AtomicBoolean(false);
        shutdownLatch = new CountDownLatch(1);

        /* start scheduler */
        kafkaScheduler.startup();

        /* start log manager */
        logManager = new LogManager(config,
                kafkaScheduler,
                millis);
        logManager.startup();

        socketServer = new SocketServer(config.brokerId,
                config.hostName,
                config.port,
                config.numNetworkThreads,
                config.queuedMaxRequests,
                config.socketSendBufferBytes,
                config.socketReceiveBufferBytes,
                config.socketRequestMaxBytes);

        socketServer.startup();

        /* start client */
        kafkaZookeeper = new KafkaZooKeeper(config);
        // starting relevant replicas and leader election for partitions assigned to this broker
        kafkaZookeeper.startup();

        logger.info("Connecting to ZK: " + config.zkConnect);

        replicaManager = new ReplicaManager(config, millis, kafkaZookeeper.getZookeeperClient(), kafkaScheduler, logManager, isShuttingDown);

        kafkaController = new KafkaController(config, kafkaZookeeper.getZookeeperClient());
        apis = new KafkaApis(socketServer.requestChannel, replicaManager, kafkaZookeeper.getZookeeperClient(), config.brokerId, kafkaController);
        requestHandlerPool = new KafkaRequestHandler.KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads);

        // start the replica manager
        replicaManager.startup();
        // start the controller
        kafkaController.startup();
        startupComplete.set(true);
        logger.info("Started");
    }


    /**
     *  Performs controlled shutdown
     */
    private void controlledShutdown() throws IOException,InterruptedException {
        if (startupComplete.get() && config.controlledShutdownEnable) {
            // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
            // of time and try again for a configured number of retries. If all the attempt fails, we simply force
            // the shutdown.
            int remainingRetries = config.controlledShutdownMaxRetries;
            logger.info("Starting controlled shutdown");
            BlockingChannel channel = null;
            Broker prevController  = null;
            boolean shutdownSuceeded  = false;
            try {
                while (!shutdownSuceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1;

                    // 1. Find the controller and establish a connection to it.

                    // Get the current controller info. This is to ensure we use the most recent info to issue the
                    // controlled shutdown request
                    int controllerId = ZkUtils.getController(kafkaZookeeper.getZookeeperClient());
                    Broker broker = ZkUtils.getBrokerInfo(kafkaZookeeper.getZookeeperClient(), controllerId) ;
                    if(broker != null){
                        if (channel == null || prevController == null || !prevController.equals(broker)) {
                            // if this is the first attempt or if the controller has changed, create a channel to the most recent
                            // controller
                            if (channel != null) {
                                channel.disconnect();
                            }
                            channel = new BlockingChannel(broker.host(), broker.port(),
                                    BlockingChannel.UseDefaultBufferSize,
                                    BlockingChannel.UseDefaultBufferSize,
                                    config.controllerSocketTimeoutMs);
                            channel.connect();
                            prevController = broker;
                        }
                    }

                    // 2. issue a controlled shutdown to the controller
                    if (channel != null) {
                        Receive response = null;
                        try {
                            // send the controlled shutdown request
                            ControlledShutdownRequest request = new ControlledShutdownRequest(correlationId.getAndIncrement(), config.brokerId);
                            channel.send(request);
                            response = channel.receive();
                            ControlledShutdownResponse shutdownResponse = ControlledShutdownResponse.readFrom(response.buffer());
                            if (shutdownResponse.errorCode == ErrorMapping.NoError && shutdownResponse.partitionsRemaining != null &&
                                    shutdownResponse.partitionsRemaining.size() == 0) {
                                shutdownSuceeded = true;
                                logger.info ("Controlled shutdown succeeded");
                            }
                            else {
                                logger.info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.toString()));
                                logger.info("Error code from controller: %d".format(shutdownResponse.errorCode+""));
                            }
                        }
                        catch (IOException ios){
                            channel.disconnect();
                            channel = null;
                            // ignore and try again
                        }
                    }
                    if (!shutdownSuceeded) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs);
                        logger.warn("Retrying controlled shutdown after the previous attempt failed...");
                    }
                }
            }
            finally {
                if (channel != null) {
                    channel.disconnect();
                    channel = null;
                }
            }
            if (!shutdownSuceeded) {
                logger.warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed");
            }
        }
    }

    /**
     * Shutdown API for shutting down a single instance of the Kafka server.
     * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
     */
    public void shutdown() {
        logger.info("Shutting down");
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (canShutdown) {
            try{
                controlledShutdown();
            }catch (Exception e){
                logger.error(e.getMessage(), e);
            }
            if(kafkaZookeeper != null)
                kafkaZookeeper.shutdown();
            if(socketServer != null) {
                try{
                    socketServer.shutdown();
                }catch (Exception e){
                    logger.error(e.getMessage(), e);
                }
            }
            if(requestHandlerPool != null) {
                try{
                    requestHandlerPool.shutdown();
                }catch (Exception e){
                    logger.error(e.getMessage(), e);
                }
            }
            kafkaScheduler.shutdown();
            if(apis != null) {
                try{
                    apis.close();
                }catch (Exception e){
                    logger.error(e.getMessage(), e);
                }
            }
            if(replicaManager != null) {
                try{
                    replicaManager.shutdown();
                }catch (Exception e){
                    logger.error(e.getMessage(), e);
                }
            }
            if(logManager != null) {
                try{
                    logManager.shutdown();
                }catch (Exception e){
                    logger.error(e.getMessage(), e);
                }
            }

            if(kafkaController != null)
                kafkaController.shutdown();

            shutdownLatch.countDown();
            startupComplete.set(false);
            logger.info("Shut down completed");
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    public LogManager getLogManager(){
        return logManager;
    }
}

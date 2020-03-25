package kafka.server;

import org.apache.log4j.Logger;

public class KafkaServerStartable {

    private static Logger logger = Logger.getLogger(KafkaServerStartable.class);

    KafkaConfig serverConfig;

    private KafkaServer server  = null;

    public KafkaServerStartable(KafkaConfig serverConfig){
        this.serverConfig = serverConfig;
        init();
    }

    public KafkaConfig serverConfig() {
        return serverConfig;
    }

    private void init() {
        server = new KafkaServer(serverConfig);
    }

    public void startup() {
        try {
            server.startup();
        }
        catch (Exception e){
            logger.fatal("Fatal error during KafkaServerStable startup. Prepare to shutdown", e);
            shutdown();
        }
    }

    public void shutdown() {
        try {
            server.shutdown();
        }
        catch (Exception e) {
            logger.fatal("Fatal error during KafkaServerStable shutdown. Prepare to halt", e);
            Runtime.getRuntime().halt(1);
        }
    }

    public void awaitShutdown() throws InterruptedException{
        server.awaitShutdown();
    }
}

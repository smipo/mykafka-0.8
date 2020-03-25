package kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Kafka {

    private static Logger logger = Logger.getLogger(Kafka.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("USAGE: java [options] %s server.properties".format(KafkaServer.class.getSimpleName()));
            System.exit(1);
        }
        try {
            Properties props = Utils.loadProps(args[0]);
            KafkaConfig serverConfig = new KafkaConfig(props);

            final KafkaServerStartable kafkaServerStartble = new KafkaServerStartable(serverConfig);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread() {
                 public void run()  {
                     try{
                         kafkaServerStartble.shutdown();
                         kafkaServerStartble.awaitShutdown();
                     }catch (InterruptedException e){
                        logger.error("shutdownHook Error:",e);
                     }
                }
            });

            kafkaServerStartble.startup();
            kafkaServerStartble.awaitShutdown();
        }
        catch (Exception e){
           logger.error("Kafka start Error:",e);
        }
        System.exit(0);
    }
}

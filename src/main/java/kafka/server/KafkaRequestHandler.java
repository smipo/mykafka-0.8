package kafka.server;

import kafka.network.RequestChannel;
import kafka.utils.Utils;
import org.apache.log4j.Logger;


/**
 * A thread that answers kafka requests.
 */
public class KafkaRequestHandler implements Runnable{

    private static Logger logger = Logger.getLogger(KafkaRequestHandler.class);

    int id;
    int brokerId;
    RequestChannel requestChannel;
    KafkaApis apis;

    public KafkaRequestHandler(int id, int brokerId, RequestChannel requestChannel, KafkaApis apis) {
        this.id = id;
        this.brokerId = brokerId;
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    @Override
    public void run() {
        while(true) {
            try {
                RequestChannel.Request req = requestChannel.receiveRequest();
                if(req == RequestChannel.AllDone) {
                    logger.debug("Kafka request handler %d on broker %d received shut down command".format(
                            id+"", brokerId));
                    return;
                }
                logger.trace("Kafka request handler %d on broker %d handling request %s".format(id+"", brokerId, req));
                apis.handle(req);
            } catch (Throwable e){
                logger.error("Exception when handling request", e);
            }
        }
    }
    public void shutdown() throws InterruptedException {
        requestChannel.sendRequest(RequestChannel.AllDone);
    }

    public static class KafkaRequestHandlerPool{
        int brokerId;
        RequestChannel requestChannel;
        KafkaApis apis;
        int numThreads;

        public KafkaRequestHandlerPool(int brokerId, RequestChannel requestChannel, KafkaApis apis, int numThreads) {
            this.brokerId = brokerId;
            this.requestChannel = requestChannel;
            this.apis = apis;
            this.numThreads = numThreads;
            threads = new Thread[numThreads];
            runnables = new KafkaRequestHandler[numThreads];
            for(int i = 0 ;i < numThreads;i++) {
                runnables[i] = new KafkaRequestHandler(i, brokerId, requestChannel, apis);
                threads[i] = Utils.daemonThread("kafka-request-handler-" + i, runnables[i]);
                threads[i].start();
            }
        }

        Thread[] threads ;
        KafkaRequestHandler[] runnables ;


        public void shutdown() throws InterruptedException {
            logger.info("shutting down");
            for(KafkaRequestHandler handler : runnables)
                handler.shutdown();
            for(Thread thread : threads)
                thread.join();
            logger.info("shutted down completely");
        }
    }


}

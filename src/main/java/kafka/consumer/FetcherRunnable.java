package kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.common.ErrorMapping;
import kafka.message.ByteBufferMessageSet;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FetcherRunnable extends Thread{

    private static Logger logger = Logger.getLogger(FetcherRunnable.class);

    String name;
    ZkClient zkClient ;
    ConsumerConfig config;
    Broker broker;
    List<PartitionTopicInfo> partitionTopicInfos;

    public FetcherRunnable(String name, ZkClient zkClient, ConsumerConfig config, Broker broker, List<PartitionTopicInfo> partitionTopicInfos) {
        super(name);
        this.name = name;
        this.zkClient = zkClient;
        this.config = config;
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        simpleConsumer = new SimpleConsumer(broker.host(), broker.port(), config.socketTimeoutMs,
                config.socketBufferSize);
    }

    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private SimpleConsumer simpleConsumer ;
    private volatile boolean stopped = false;

    public void shutdown() throws InterruptedException{
        stopped = true;
        interrupt();
        logger.debug("awaiting shutdown on fetcher " + name);
        shutdownLatch.await();
        logger.debug("shutdown of fetcher " + name + " thread complete");
    }

    @Override
    public void run() {
        for (PartitionTopicInfo infopti : partitionTopicInfos)
            logger.info(name + " start fetching topic: " + infopti.topic + " part: " + infopti.partition.partId() + " offset: "
                    + infopti.getFetchOffset() + " from " + broker.host() + ":" + broker.port());

        try {
            while (!stopped) {
                FetchRequest[] fetches = new FetchRequest[partitionTopicInfos.size()];
                for(int i = 0;i < partitionTopicInfos.size();i++ ){
                    PartitionTopicInfo info = partitionTopicInfos.get(i);
                    fetches[i] =  new FetchRequest(info.topic, info.partition.partId(), info.getFetchOffset(), config.fetchSize);
                }
                String input = Arrays.asList(fetches).toString();
                logger.info("fetch request: " +  input);
                MultiFetchResponse response = simpleConsumer.multifetch(fetches);
                logger.info("recevied response from fetch request: " + input);
                long read = 0L;
                List<ByteBufferMessageSet> messagesList = new ArrayList<>();
                Iterator<ByteBufferMessageSet> iterator = response.iterator();
                while (iterator.hasNext()){
                    messagesList.add(iterator.next());
                }
                for(int i = 0;i < messagesList.size();i++){
                    if(i > partitionTopicInfos.size() - 1) break;
                    ByteBufferMessageSet messages = messagesList.get(i);
                    PartitionTopicInfo infopti = partitionTopicInfos.get(i);
                    try {
                        boolean done = false;
                        if(messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode) {
                            logger.info("offset for " + infopti + " out of range");
                            // see if we can fix this error
                            long resetOffset = resetConsumerOffsets(infopti.topic, infopti.partition);
                            if(resetOffset >= 0) {
                                infopti.resetFetchOffset(resetOffset);
                                infopti.resetConsumeOffset(resetOffset);
                                done = true;
                            }
                        }
                        if (!done)
                            read += infopti.enqueue(messages, infopti.getFetchOffset());
                    }
                    catch (IOException e1){
                        // something is wrong with the socket, re-throw the exception to stop the fetcher
                        throw e1;
                    }catch (Exception e2){
                        if (!stopped) {
                            // this is likely a repeatable error, log it and trigger an exception in the consumer
                            logger.error("error in FetcherRunnable for " + infopti, e2);
                            infopti.enqueueError(e2, infopti.getFetchOffset());
                        }
                        // re-throw the exception to stop the fetcher
                        throw e2;
                    }
                }
                logger.trace("fetched bytes: " + read);
                if(read == 0) {
                    logger.debug("backing off " + config.fetcherBackoffMs + " ms");
                    Thread.sleep(config.fetcherBackoffMs);
                }
            }
        }
        catch (Exception e){
            if (stopped)
                logger.info("FecherRunnable " + this + " interrupted");
            else
                logger.error("error in FetcherRunnable ", e);
        }
        logger.info("stopping fetcher " + name + " to host " + broker.host());
        try {
            simpleConsumer.close();
        }catch (IOException e){
            logger.error("fetcherRunnable simpleConsumer close Error:",e);
        }
        shutdownComplete();
    }

    /**
     * Record that the thread shutdown is complete
     */
    private void shutdownComplete() {
        shutdownLatch.countDown();
    }

    private long resetConsumerOffsets(String topic ,
                                      Partition partition) throws IOException {
        long offset  = -1;
        if(OffsetRequest.LargestTimeString == config.autoOffsetReset){
            offset = OffsetRequest.LatestTime;
        }else if(OffsetRequest.SmallestTimeString == config.autoOffsetReset){
            offset = OffsetRequest.EarliestTime;
        }

        // get mentioned offset from the broker
        long[] offsets = simpleConsumer.getOffsetsBefore(topic, partition.partId(), offset, 1);
        ZkUtils.ZKGroupTopicDirs topicDirs = new ZkUtils.ZKGroupTopicDirs(config.groupId, topic);

        // reset manually in zookeeper
        String input = "latest";
        if(offset == OffsetRequest.EarliestTime) input = "earliest ";
        logger.info("updating partition " + partition.name() + " for topic " + topic + " with " +
                input + "offset " + offsets[0]);
        ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition.name(), String.valueOf(offsets[0]));

       return offsets[0];
    }
}

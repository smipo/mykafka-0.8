package kafka.server;

import kafka.api.FetchRequest;
import kafka.api.FetchResponse;
import kafka.cluster.Broker;
import kafka.common.ClientIdAndBroker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.consumer.PartitionTopicInfo;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
import kafka.utils.ShutdownableThread;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractFetcherThread extends ShutdownableThread {

    private static Logger logger = Logger.getLogger(AbstractFetcherThread.class);

    public  String clientId;
    public  Broker sourceBroker;
    public int socketTimeout;
    public int socketBufferSize;
    public int fetchSize;
    public int fetcherBrokerId;
    public int  maxWait;
    public int  minBytes;

    public AbstractFetcherThread(String name, String clientId, Broker sourceBroker, int socketTimeout, int socketBufferSize, int fetchSize, int fetcherBrokerId, int maxWait, int minBytes, boolean isInterruptible) {
        super(name,isInterruptible);
        this.clientId = clientId;
        this.sourceBroker = sourceBroker;
        this.socketTimeout = socketTimeout;
        this.socketBufferSize = socketBufferSize;
        this.fetchSize = fetchSize;
        this.fetcherBrokerId = fetcherBrokerId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        simpleConsumer = new SimpleConsumer(sourceBroker.host(), sourceBroker.port(), socketTimeout, socketBufferSize, clientId);
        brokerInfo = "host_%s-port_%s".format(sourceBroker.host(), sourceBroker.port());
        metricId = new ClientIdAndBroker(clientId, brokerInfo);

        fetchRequestBuilder = new FetchRequest.FetchRequestBuilder().
                clientId(clientId).
                replicaId(fetcherBrokerId).
                maxWait(maxWait).
                minBytes(minBytes);
    }


    private Map<TopicAndPartition, Long> partitionMap = new HashMap<>(); // a (topic, partition) -> offset map
    private ReentrantLock partitionMapLock = new ReentrantLock();
    private Condition partitionMapCond = partitionMapLock.newCondition();
    SimpleConsumer simpleConsumer ;
    private String brokerInfo ;
    private ClientIdAndBroker metricId ;
    FetchRequest.FetchRequestBuilder fetchRequestBuilder ;

    /* callbacks to be defined in subclass */

    // process fetched data
    abstract void processPartitionData(TopicAndPartition topicAndPartition, long fetchOffset,
                                       FetchResponse.FetchResponsePartitionData partitionData);

    // handle a partition whose offset is out of range and return a new fetch offset
    abstract long handleOffsetOutOfRange(TopicAndPartition topicAndPartition);

    // deal with partitions with errors, potentially due to leadership changes
    abstract void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions);

    @Override
    public void shutdown() throws IOException, InterruptedException {
        super.shutdown();
        simpleConsumer.close();
    }

    @Override
    public void doWork() throws IOException, InterruptedException{
        partitionMapLock.lock();
        try{
            if (partitionMap.isEmpty())
                partitionMapCond.await(200L, TimeUnit.MILLISECONDS);
            for(Map.Entry<TopicAndPartition, Long> entry : partitionMap.entrySet()){
                fetchRequestBuilder.addFetch(entry.getKey().topic(), entry.getKey().partition(),
                        entry.getValue(), fetchSize);
            }
        }finally {
            partitionMapLock.unlock();
        }
        FetchRequest fetchRequest = fetchRequestBuilder.build();
        if (!fetchRequest.requestInfo.isEmpty())
            processFetchRequest(fetchRequest);
    }

    private void processFetchRequest(FetchRequest fetchRequest) {
        Set<TopicAndPartition> partitionsWithError = new HashSet<>();
        FetchResponse response = null;
        try {
            logger.info("issuing to broker %d of fetch request %s".format(sourceBroker.id()+"", fetchRequest));
            response = simpleConsumer.fetch(fetchRequest);
        } catch (Throwable t){
                if (isRunning.get()) {
                    logger.warn("Error in fetch %s".format(fetchRequest.toString()), t);
                     synchronized(partitionMapLock) {
                        partitionsWithError.addAll(partitionMap.keySet());
                    }
                }
        }
        if (response != null) {
            // process fetched data
            partitionMapLock.lock();
            try{
                for(Map.Entry<TopicAndPartition, FetchResponse.FetchResponsePartitionData> entry : response.data.entrySet()){
                    TopicAndPartition topicAndPartition = entry.getKey();
                    FetchResponse.FetchResponsePartitionData partitionData = entry.getValue();
                    String topic = topicAndPartition.topic();
                    int partitionId = topicAndPartition.partition();
                    Long currentOffset = partitionMap.get(topicAndPartition);
                    // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
                    if (currentOffset == null && fetchRequest.requestInfo.get(topicAndPartition).offset == currentOffset) {
                        if(partitionData.error ==  ErrorMapping.NoError){
                            try {
                                ByteBufferMessageSet messages = (ByteBufferMessageSet)partitionData.messages;
                                long validBytes = messages.validBytes();
                                Long newOffset = currentOffset;
                                while (messages.shallowIterator().hasNext()){
                                    newOffset = messages.shallowIterator().next().nextOffset();
                                }
                                partitionMap.put(topicAndPartition, newOffset);
                                // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                                processPartitionData(topicAndPartition, currentOffset, partitionData);
                            } catch (InvalidMessageException ime){
                                    // we log the error and continue. This ensures two things
                                    // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                                    // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                                    //    should get fixed in the subsequent fetches
                                    logger.warn("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentOffset + " error " + ime.getMessage());

                            }catch (Throwable e){
                                throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                                        .format(topic, partitionId, currentOffset), e);
                            }
                        }else if(partitionData.error == ErrorMapping.OffsetOutOfRangeCode){
                            try {
                                long newOffset = handleOffsetOutOfRange(topicAndPartition);
                                partitionMap.put(topicAndPartition, newOffset);
                                logger.warn("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                                        .format(currentOffset+"", topic, partitionId, newOffset));
                            } catch (Throwable e){
                                    logger.warn("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id()), e);
                                    partitionsWithError .add(topicAndPartition);
                            }
                        }else{
                            if (isRunning.get()) {
                                logger.warn("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id());
                                partitionsWithError.add(topicAndPartition);
                            }
                        }
                    }
                }
            }finally {
                partitionMapLock.unlock();
            }
        }

        if(partitionsWithError.size() > 0) {
            logger.debug("handling partitions with error for %s".format(partitionsWithError.toString()));
            handlePartitionsWithErrors(partitionsWithError);
        }
    }

    public void addPartition(String topic, int partitionId, long initialOffset) throws InterruptedException {
        partitionMapLock.lockInterruptibly();
        try {
            TopicAndPartition topicPartition = new TopicAndPartition(topic, partitionId);
            partitionMap.put(
                    topicPartition,
            if (PartitionTopicInfo.isOffsetInvalid(initialOffset)) handleOffsetOutOfRange(topicPartition) else initialOffset)
            partitionMapCond.signalAll();
        } finally {
            partitionMapLock.unlock();
        }
    }

    public void removePartition(String topic, int partitionId) throws InterruptedException {
        partitionMapLock.lockInterruptibly();
        try {
            partitionMap.remove(new TopicAndPartition(topic, partitionId));
        } finally {
            partitionMapLock.unlock();
        }
    }

    public int partitionCount() throws InterruptedException {
        partitionMapLock.lockInterruptibly();
        try {
            return partitionMap.size();
        } finally {
            partitionMapLock.unlock();
        }
    }
}

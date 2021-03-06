package kafka.server;

import kafka.api.LeaderAndIsrRequest;
import kafka.api.StopReplicaRequest;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.log.LogManager;
import kafka.utils.KafkaScheduler;
import kafka.utils.Pair;
import kafka.utils.Pool;
import kafka.utils.Three;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class ReplicaManager {

    private static Logger logger = Logger.getLogger(ReplicaManager.class);

    public static long UnknownLogEndOffset = -1L;

    public KafkaConfig config;
    public long millisTime;
    public ZkClient zkClient;
    public KafkaScheduler kafkaScheduler;
    public LogManager logManager;
    public AtomicBoolean isShuttingDown;

    public ReplicaManager(KafkaConfig config, long millisTime, ZkClient zkClient, KafkaScheduler kafkaScheduler, LogManager logManager, AtomicBoolean isShuttingDown){
        this.config = config;
        this.millisTime = millisTime;
        this.zkClient = zkClient;
        this.kafkaScheduler = kafkaScheduler;
        this.logManager = logManager;
        this.isShuttingDown = isShuttingDown;

        localBrokerId = config.brokerId;
        replicaFetcherManager = new ReplicaFetcherManager(config, this);
        for(String dir:config.logDirs){
            highWatermarkCheckpoints.put(dir, new HighwaterMarkCheckpoint(dir));
        }
    }

    volatile int controllerEpoch = KafkaController.InitialControllerEpoch - 1;
    private int localBrokerId;
    private Pool<Pair<String, Integer>, Partition> allPartitions = new Pool<>();
    private Set<Partition> leaderPartitions = new HashSet<>();
    private Object leaderPartitionsLock = new Object();
    public ReplicaFetcherManager replicaFetcherManager;
    private AtomicBoolean highWatermarkCheckPointThreadStarted = new AtomicBoolean(false);
    public Map<String,HighwaterMarkCheckpoint> highWatermarkCheckpoints = new HashMap<>();
    private boolean hwThreadInitialized = false;


    public void startHighWaterMarksCheckPointThread()  {
        if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
            kafkaScheduler.scheduleWithRate(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(kafkaScheduler.currentThreadName("highwatermark-checkpoint-thread"));
                    try {
                        checkpointHighWatermarks();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            },  0, config.replicaHighWatermarkCheckpointIntervalMs, true);
    }

    /**
     * This function is only used in two places: in Partition.updateISR() and KafkaApis.handleProducerRequest().
     * In the former case, the partition should have been created, in the latter case, return -1 will put the request into purgatory
     */
    public int getReplicationFactorForPartition(String topic,int partitionId)  {
        Partition partitionOpt = getPartition(topic, partitionId);
        if(partitionOpt != null){
            return partitionOpt.replicationFactor;
        }
        return -1;
    }

    public void startup() {
        // start ISR expiration thread
        kafkaScheduler.scheduleWithRate(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName(kafkaScheduler.currentThreadName("isr-expiration-thread-"));
                maybeShrinkIsr();
            }},  0, config.replicaLagTimeMaxMs,false);
    }

    public short stopReplica(String topic,int partitionId, boolean deletePartition) throws InterruptedException {
        logger.trace(String.format("Broker %d handling stop replica for partition [%s,%d]",localBrokerId, topic, partitionId));
        short errorCode = ErrorMapping.NoError;
        Replica replica = getReplica(topic, partitionId,config.brokerId) ;
        if(replica != null){
            replicaFetcherManager.removeFetcher(topic, partitionId);
            /* TODO: handle deleteLog in a better way */
            //if (deletePartition)
            //  logManager.deleteLog(topic, partition)
            synchronized(leaderPartitionsLock) {
                leaderPartitions.remove(replica.partition);
            }
            if(deletePartition)
                allPartitions.remove(new Pair<>(topic, partitionId));
        }
        logger.trace(String.format("Broker %d finished handling stop replica for partition [%s,%d]",localBrokerId, topic, partitionId));
        return errorCode;
    }

    public Pair<Map<Pair<String, Integer>,Short>, Short> stopReplicas(StopReplicaRequest stopReplicaRequest) throws InterruptedException {
        Map<Pair<String, Integer>,Short> responseMap = new HashMap();
        if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
            logger.warn(String
                    .format("Broker %d received stop replica request from an old controller epoch %d.",localBrokerId, stopReplicaRequest.controllerEpoch) +
                    " Latest known controller epoch is %d " + controllerEpoch);
            return new Pair<>(responseMap, ErrorMapping.StaleControllerEpochCode);
        } else {
            controllerEpoch = stopReplicaRequest.controllerEpoch;
            for(Pair<String, Integer> p:stopReplicaRequest.partitions){
                short errorCode = stopReplica(p.getKey(), p.getValue(), stopReplicaRequest.deletePartitions);
                responseMap.put(new Pair<>(p.getKey(), p.getValue()), errorCode);
            }
            return new Pair<>(responseMap, ErrorMapping.NoError);
        }
    }

    public Partition getOrCreatePartition(String topic,int partitionId,int replicationFactor) {
        Partition partition = allPartitions.get(new Pair<>(topic, partitionId));
        if (partition == null) {
            allPartitions.putIfNotExists(new Pair<>(topic, partitionId), new Partition(topic, partitionId, replicationFactor, millisTime, this));
            partition = allPartitions.get(new Pair<>(topic, partitionId));
        }
        return partition;
    }

    public Partition getPartition(String topic, int partitionId) {
        Partition partition = allPartitions.get(new Pair<>(topic, partitionId));
        return partition;
    }

    public Replica getReplicaOrException(String topic,int partition) {
        Replica replicaOpt = getReplica(topic, partition,config.brokerId);
        if(replicaOpt != null)
            return replicaOpt;
        else
            throw new ReplicaNotAvailableException(String.format("Replica %d is not available for partition [%s,%d]",config.brokerId, topic, partition));
    }

    public Replica getLeaderReplicaIfLocal(String topic, int partitionId) {
        Partition partition = getPartition(topic, partitionId);
        if(partition == null){
            throw new UnknownTopicOrPartitionException(String.format("Partition [%s,%d] doesn't exist on %d",topic, partitionId, config.brokerId));
        }
        if(partition.leaderReplicaIfLocal() == null){
            throw new NotLeaderForPartitionException(String
                    .format("Leader not local for partition [%s,%d] on broker %d",topic, partitionId, config.brokerId));
        }
        return partition.leaderReplicaIfLocal();
    }

    public Replica getReplica(String topic,int partitionId){
        return getReplica(topic,partitionId,config.brokerId);
    }
    public Replica getReplica(String topic,int partitionId, int replicaId)  {
        Partition partitionOpt = getPartition(topic, partitionId);
        if(partitionOpt != null){
            return partitionOpt.getReplica(replicaId);
        }
        return null;
    }

    public Pair<Map<Pair<String, Integer>,Short>, Short> becomeLeaderOrFollower(LeaderAndIsrRequest leaderAndISRRequest) throws IOException, InterruptedException {
        Map<Pair<String, Integer>,Short> responseMap = new HashMap<>();
        if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
            logger.warn(String
                    .format("Broker %d received LeaderAndIsr request correlation id %d with an old controller epoch %d. Latest known controller epoch is %d",localBrokerId, leaderAndISRRequest.controllerEpoch, leaderAndISRRequest.correlationId, controllerEpoch));
            return  new Pair<>(responseMap, ErrorMapping.StaleControllerEpochCode);
        }else {
            int controllerId = leaderAndISRRequest.controllerId;
            controllerEpoch = leaderAndISRRequest.controllerEpoch;
            for(Map.Entry<Pair<String, Integer>, LeaderAndIsrRequest.PartitionStateInfo> entry :  leaderAndISRRequest.partitionStateInfos.entrySet()){
                short errorCode = ErrorMapping.NoError;
                String topic = entry.getKey().getKey();
                int partitionId = entry.getKey().getValue();

                int requestedLeaderId = entry.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader;
                try {
                    if(requestedLeaderId == config.brokerId)
                        makeLeader(controllerId, controllerEpoch, topic, partitionId, entry.getValue(), leaderAndISRRequest.correlationId);
                    else
                        makeFollower(controllerId, controllerEpoch, topic, partitionId, entry.getValue(), leaderAndISRRequest.leaders,
                                leaderAndISRRequest.correlationId);
                } catch (Throwable e){
                        String errorMsg = String.format("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %s " +
                                        "epoch %d for partition %s",localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
                                leaderAndISRRequest.controllerEpoch, entry.toString());
                        logger.error(errorMsg, e);
                        errorCode = ErrorMapping.codeFor(e.getClass().getName());
                }
                responseMap.put(entry.getKey(), errorCode);
                logger.trace(String
                        .format("Broker %d handled LeaderAndIsr request correlationId %d received from controller %d epoch %d for partition [%s,%d]",localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch,
                                entry.getKey().getKey(), entry.getKey().getValue()));
            }
            logger.info(String.format("Handled leader and isr request %s",leaderAndISRRequest.toString()));
            // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
            // have been completely populated before starting the checkpointing there by avoiding weird race conditions
            if (!hwThreadInitialized) {
                startHighWaterMarksCheckPointThread();
                hwThreadInitialized = true;
            }
            replicaFetcherManager.shutdownIdleFetcherThreads();
            return new Pair<>(responseMap, ErrorMapping.NoError);
        }
    }

    private void makeLeader(int controllerId, int epoch, String topic, int partitionId,
                            LeaderAndIsrRequest.PartitionStateInfo partitionStateInfo,int correlationId) throws IOException, InterruptedException {
        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
        logger.trace(String
                .format("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                        "starting the become-leader transition for partition [%s,%d]",localBrokerId, correlationId, controllerId, epoch, topic, partitionId));
        Partition partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor());
        if (partition.makeLeader(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, correlationId)) {
            // also add this partition to the list of partitions for which the leader is the current broker
             synchronized(leaderPartitionsLock) {
                leaderPartitions.add(partition);
            }
        }
        logger.trace(String.format("Broker %d completed become-leader transition for partition [%s,%d]",localBrokerId, topic, partitionId));
    }

    private void makeFollower(int controllerId, int epoch, String topic, int partitionId,
                              LeaderAndIsrRequest.PartitionStateInfo partitionStateInfo, Set<Broker> leaders, int correlationId) throws Throwable {
        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
        logger.trace(String
                .format("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                        "starting the become-follower transition for partition [%s,%d]",localBrokerId, correlationId, controllerId, epoch, topic, partitionId));

        Partition partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor());
        if (partition.makeFollower(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, leaders, correlationId)) {
            // remove this replica's partition from the ISR expiration queue
             synchronized(leaderPartitionsLock) {
                leaderPartitions.remove(partition);
            }
        }
        logger.trace(String.format("Broker %d completed the become-follower transition for partition [%s,%d]",localBrokerId, topic, partitionId));
    }

    private void maybeShrinkIsr() {
        logger.trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR");
        List<Partition> curLeaderPartitions = null;
        synchronized(leaderPartitionsLock) {
            curLeaderPartitions = leaderPartitions.stream().collect(Collectors.toList());
        }
        for(Partition partition:curLeaderPartitions){
            partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages);
        }
    }

    public void recordFollowerPosition(String topic, int partitionId, int replicaId, long offset) throws IOException {
        Partition partitionOpt = getPartition(topic, partitionId);
        if(partitionOpt != null) {
            partitionOpt.updateLeaderHWAndMaybeExpandIsr(replicaId, offset);
        } else {
            logger.warn(String.format("While recording the follower position, the partition [%s,%d] hasn't been created, skip updating leader HW",topic, partitionId));
        }
    }

    /**
     * Flushes the highwatermark value for all partitions to the highwatermark file
     */
    public void checkpointHighWatermarks() throws IOException {
        List<Replica> replicas = new ArrayList<>();
        for(Partition partition:allPartitions.values()){
            Replica  replica = partition.getReplica(config.brokerId);
            if(replica != null){
                replicas.add(replica);
            }
        }
        Map<String,List<Replica>> replicasByDir = replicas.stream().filter(r->r.log != null).collect(Collectors.groupingBy(r->r.log.dir().getParent()));
        for(Map.Entry<String,List<Replica>> entry : replicasByDir.entrySet()){
            Map<TopicAndPartition, Long> highwaterMarksPerPartition = new HashMap<>();
            for(Replica r:entry.getValue()){
                highwaterMarksPerPartition.put(new TopicAndPartition(r.topic, r.partitionId), r.highWatermark());
            }
            highWatermarkCheckpoints.get(entry.getKey().replace("\\","/")).write(highwaterMarksPerPartition);
        }
    }

    public void shutdown() throws IOException, InterruptedException {
        logger.info("Shut down");
        replicaFetcherManager.shutdown();
        checkpointHighWatermarks();
        logger.info("Shutted down completely");
    }
}

package kafka.cluster;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.ErrorMapping;
import kafka.common.NotLeaderForPartitionException;
import kafka.controller.KafkaController;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.message.ByteBufferMessageSet;
import kafka.server.ReplicaFetcherManager;
import kafka.server.ReplicaManager;
import kafka.utils.Pair;
import kafka.utils.Pool;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


public class Partition {

    private static Logger logger = Logger.getLogger(Partition.class);

    public String topic;
    public int partitionId;
    public int replicationFactor;
    public long milliseconds;
    public ReplicaManager replicaManager;

    public Partition(String topic, int partitionId, int replicationFactor, long milliseconds, ReplicaManager replicaManager) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
        this.milliseconds = milliseconds;
        this.replicaManager = replicaManager;

        localBrokerId = replicaManager.config.brokerId;
        logManager = replicaManager.logManager;
        replicaFetcherManager = replicaManager.replicaFetcherManager;
        zkClient = replicaManager.zkClient;
        zkVersion = LeaderAndIsrRequest.LeaderAndIsr.initialZKVersion;
        leaderEpoch = LeaderAndIsrRequest.LeaderAndIsr.initialLeaderEpoch - 1;
    }


    public  int localBrokerId ;
    private LogManager logManager;
    private ReplicaFetcherManager replicaFetcherManager ;
    private ZkClient zkClient;
    public  Integer leaderReplicaIdOpt = null;
    public  Set<Replica> inSyncReplicas = new HashSet<>();
    private Pool<Integer,Replica> assignedReplicaMap = new Pool<>();
    private Object leaderIsrUpdateLock = new Object();
    private int zkVersion;
    private int leaderEpoch;
    /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
     * One way of doing that is through the controller's start replica state change command. When a new broker starts up
     * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
     * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
     * each partition. */
    private int controllerEpoch = KafkaController.InitialControllerEpoch - 1;

    private boolean isReplicaLocal(int replicaId) {
        return replicaId == localBrokerId;
    }

    public Replica getOrCreateReplica(int replicaId) throws IOException {
        Replica replicaOpt = getReplica(replicaId);
        if(replicaOpt != null){
            return replicaOpt;
        }
        if (isReplicaLocal(replicaId)) {
            Log log = logManager.getOrCreateLog(topic, partitionId);
            long offset = Math.min(log.logEndOffset(),replicaManager.highWatermarkCheckpoints.get(log.dir().getParent()).read(topic, partitionId));
            Replica localReplica = new Replica(replicaId, this, milliseconds, offset, log);
            addReplicaIfNotExists(localReplica);
        }
        else {
            Replica remoteReplica = new Replica(replicaId, this, milliseconds,0,null);
            addReplicaIfNotExists(remoteReplica);
        }
        return getReplica(replicaId);
    }

    public Replica getReplica(int replicaId) {
        Replica replica = assignedReplicaMap.get(replicaId);
        return replica;
    }

    public Replica leaderReplicaIfLocal() {
        synchronized(leaderIsrUpdateLock) {
            if(leaderReplicaIdOpt != null){
                if (leaderReplicaIdOpt == localBrokerId)
                    return getReplica(localBrokerId);
            }
            return null;
        }
    }

    public void addReplicaIfNotExists(Replica replica)  {
        assignedReplicaMap.putIfNotExists(replica.brokerId, replica);
    }

    public  Set<Replica> assignedReplicas() {
        return assignedReplicaMap.values().stream().collect(Collectors.toSet());
    }


    /**
     *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the leader in the following steps.
     *  1. stop the existing replica fetcher
     *  2. create replicas in ISR if needed (the ISR expand/shrink logic needs replicas in ISR to be available)
     *  3. reset LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
     *  4. set the new leader and ISR
     */
    public boolean makeLeader(int controllerId, String topic, int partitionId,
                              LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,int correlationId) throws InterruptedException, IOException {
        synchronized(leaderIsrUpdateLock) {
            LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
            if (leaderEpoch >= leaderAndIsr.leaderEpoch){
                logger.trace(("Broker %d discarded the become-leader request with correlation id %d from " +
                        "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
                        .format(localBrokerId+"", correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
                                partitionId, leaderEpoch, leaderAndIsr.leaderEpoch));
                return false;
            }
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
            // to maintain the decision maker controller's epoch in the zookeeper path
            controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
            // stop replica fetcher thread, if any
            replicaFetcherManager.removeFetcher(topic, partitionId);
            Set<Replica> newInSyncReplicas = new HashSet<>();
            for(Integer r:leaderAndIsr.isr){
                newInSyncReplicas.add(getOrCreateReplica(r));
            }
            // reset LogEndOffset for remote replicas
            for(Replica r: assignedReplicas()){
                if (r.brokerId != localBrokerId) r.logEndOffset(ReplicaManager.UnknownLogEndOffset);
            }
            inSyncReplicas = newInSyncReplicas;
            leaderEpoch = leaderAndIsr.leaderEpoch;
            zkVersion = leaderAndIsr.zkVersion;
            leaderReplicaIdOpt = localBrokerId;
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(getReplica(localBrokerId));
            return true;
        }
    }

    /**
     *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the follower in the following steps.
     *  1. stop any existing fetcher on this partition from the local replica
     *  2. make sure local replica exists and truncate the log to high watermark
     *  3. set the leader and set ISR to empty
     *  4. start a fetcher to the new leader
     */
    public boolean makeFollower(int controllerId, String topic, int partitionId,
                                LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch, Set<Broker> leaders,int correlationId) throws Throwable {
        synchronized(leaderIsrUpdateLock) {
            LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
            if (leaderEpoch >= leaderAndIsr.leaderEpoch) {
                logger.trace(("Broker %d discarded the become-follower request with correlation id %d from " +
                        "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
                        .format(localBrokerId+"", correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
                                partitionId, leaderEpoch, leaderAndIsr.leaderEpoch));
                return false;
            }
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
            // to maintain the decision maker controller's epoch in the zookeeper path
            controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
            // make sure local replica exists. This reads the last check pointed high watermark from disk. On startup, it is
            // important to ensure that this operation happens for every single partition in a leader and isr request, else
            // some high watermark values could be overwritten with 0. This leads to replicas fetching from the earliest offset
            // on the leader
            Replica localReplica = getOrCreateReplica(localBrokerId);
            int newLeaderBrokerId = leaderAndIsr.leader;
            // TODO: Delete leaders from LeaderAndIsrRequest in 0.8.1
            Broker leaderBroker = null;
            for(Broker broker:leaders){
                if(broker.id() == newLeaderBrokerId){
                    leaderBroker = broker;
                    break;
                }
            }
            if(leaderBroker != null){
                // stop fetcher thread to previous leader
                replicaFetcherManager.removeFetcher(topic, partitionId);
                localReplica.log.truncateTo(localReplica.highWatermark());
                inSyncReplicas = new HashSet<>();
                leaderEpoch = leaderAndIsr.leaderEpoch;
                zkVersion = leaderAndIsr.zkVersion;
                leaderReplicaIdOpt = newLeaderBrokerId;
                if (!replicaManager.isShuttingDown.get()) {
                    // start fetcher thread to current leader if we are not shutting down
                    replicaFetcherManager.addFetcher(topic, partitionId, localReplica.logEndOffset(), leaderBroker);
                }
                else {
                    logger.trace(("Broker %d ignored the become-follower state change with correlation id %d from " +
                            "controller %d epoch %d since it is shutting down")
                            .format(localBrokerId+"", correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch));
                }
            }else{
                logger.error(("Broker %d aborted the become-follower state change with correlation id %d from " +
                        "controller %d epoch %d for partition [%s,%d] new leader %d")
                        .format(localBrokerId+"", correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
                                topic, partitionId, newLeaderBrokerId));
            }
            return true;
        }
    }

    public void updateLeaderHWAndMaybeExpandIsr(int replicaId,long offset) throws IOException {
         synchronized(leaderIsrUpdateLock) {
            logger.debug("Recording follower %d position %d for partition [%s,%d].".format(replicaId+"", offset, topic, partitionId));
            getOrCreateReplica(replicaId).logEndOffset(offset);

            // check if this replica needs to be added to the ISR
             Replica leaderReplica = leaderReplicaIfLocal();
             if(leaderReplica != null){
                 Replica replica = getReplica(replicaId);
                 long leaderHW = leaderReplica.highWatermark();
                 if (!inSyncReplicas.contains(replica) && replica.logEndOffset() >= leaderHW) {
                     // expand ISR
                     inSyncReplicas.add(replica);
                     logger.info("Expanding ISR for partition [%s,%d] from %s to %s"
                             .format(topic, partitionId, inSyncReplicas.toString(), inSyncReplicas.toString()));
                     // update ISR in ZK and cache
                     updateIsr(inSyncReplicas);
                 }
                 maybeIncrementLeaderHW(leaderReplica);
             }
        }
    }

    public Pair<Boolean, Short> checkEnoughReplicasReachOffset(long requiredOffset, int requiredAcks){
         synchronized(leaderIsrUpdateLock) {
            if(leaderReplicaIfLocal()!= null){
                int numAcks = 0;
                for(Replica r:inSyncReplicas){
                    if (!r.isLocal())
                        if(r.logEndOffset() >= requiredOffset){
                            numAcks++;
                        }
                    else
                        numAcks++; /* also count the local (leader) replica */
                }
               logger.trace("%d/%d acks satisfied for %s-%d".format(numAcks+"", requiredAcks, topic, partitionId));
                if ((requiredAcks < 0 && numAcks >= inSyncReplicas.size()) ||
                        (requiredAcks > 0 && numAcks >= requiredAcks)) {
                    /*
                     * requiredAcks < 0 means acknowledge after all replicas in ISR
                     * are fully caught up to the (local) leader's offset
                     * corresponding to this produce request.
                     */
                    return new Pair<>(true, ErrorMapping.NoError);
                } else
                    return new Pair<>(false, ErrorMapping.NoError);
            }else{
                return new Pair<>(false, ErrorMapping.NotLeaderForPartitionCode);
            }
        }
    }

    /**
     * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
     * @param leaderReplica
     */
    private void maybeIncrementLeaderHW(Replica leaderReplica) {
        Set<Long> allLogEndOffsets = inSyncReplicas.stream().map(r->r.logEndOffset()).collect(Collectors.toSet());
        long newHighWatermark = -1;
        for(Long endOffset:allLogEndOffsets){
            if(newHighWatermark < endOffset){
                newHighWatermark = endOffset;
            }
        }
        long oldHighWatermark = leaderReplica.highWatermark();
        if(newHighWatermark > oldHighWatermark) {
            leaderReplica.highWatermark_(newHighWatermark) ;
            logger.debug("Highwatermark for partition [%s,%d] updated to %d".format(topic, partitionId, newHighWatermark));
        }
        else
            logger.debug("Old hw for partition [%s,%d] is %d. New hw is %d. All leo's are %s"
                    .format(topic, partitionId, oldHighWatermark, newHighWatermark, allLogEndOffsets));
    }

    public void maybeShrinkIsr(long replicaMaxLagTimeMs, long replicaMaxLagMessages) {
         synchronized(leaderIsrUpdateLock) {
             Replica leaderReplica = leaderReplicaIfLocal();
            if(leaderReplica != null){
                Set<Replica> outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages);
                if(outOfSyncReplicas.size() > 0) {
                    inSyncReplicas.remove(outOfSyncReplicas);
                    assert(inSyncReplicas.size() > 0);
                    logger.info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
                            inSyncReplicas.toString(), inSyncReplicas.toString()));
                    // update ISR in zk and in cache
                    updateIsr(inSyncReplicas);
                    // we may need to increment high watermark since ISR could be down to 1
                    maybeIncrementLeaderHW(leaderReplica);
                }
            }
        }
    }

    public Set<Replica> getOutOfSyncReplicas(Replica leaderReplica,long keepInSyncTimeMs, long keepInSyncMessages) {
        /**
         * there are two cases that need to be handled here -
         * 1. Stuck followers: If the leo of the replica is less than the leo of leader and the leo hasn't been updated
         *                     for keepInSyncTimeMs ms, the follower is stuck and should be removed from the ISR
         * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
         *                     follower is not catching up and should be removed from the ISR
         **/
        long leaderLogEndOffset = leaderReplica.logEndOffset();
        inSyncReplicas.remove(leaderReplica);
        // Case 1 above
        Set<Replica> possiblyStuckReplicas = inSyncReplicas.stream().filter(r -> r.logEndOffset() < leaderLogEndOffset).collect(Collectors.toSet());
        if(possiblyStuckReplicas.size() > 0)
            logger.debug("Possibly stuck replicas for partition [%s,%d] are %s".format(topic, partitionId,
                    possiblyStuckReplicas.toString()));
        Set<Replica> stuckReplicas = possiblyStuckReplicas.stream().filter(r -> r.logEndOffsetUpdateTimeMs() < (milliseconds - keepInSyncTimeMs)).collect(Collectors.toSet());
        if(stuckReplicas.size() > 0)
            logger.debug("Stuck replicas for partition [%s,%d] are %s".format(topic, partitionId, stuckReplicas.toString()));
        // Case 2 above
        Set<Replica> slowReplicas = inSyncReplicas.stream().filter(r -> r.logEndOffset() >= 0 && (leaderLogEndOffset - r.logEndOffset()) > keepInSyncMessages).collect(Collectors.toSet());
        if(slowReplicas.size() > 0)
            logger.debug("Slow replicas for partition [%s,%d] are %s".format(topic, partitionId, slowReplicas.toString()));
        stuckReplicas.addAll(slowReplicas);
        return stuckReplicas;
    }

    public Pair<Long,Long> appendMessagesToLeader(ByteBufferMessageSet messages) {
        synchronized(leaderIsrUpdateLock) {
            Replica leaderReplicaOpt = leaderReplicaIfLocal();
            if(leaderReplicaOpt == null){
                throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                        .format(topic, partitionId, localBrokerId));
            }
            Log log = leaderReplicaOpt.log;
            Pair<Long,Long> pair = log.append(messages,  true);
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplicaOpt);
            return pair;
        }
    }

    private void updateIsr(Set<Replica> newIsr) {
        logger.debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newIsr.toString()));
        LeaderAndIsrRequest.LeaderAndIsr newLeaderAndIsr = new LeaderAndIsrRequest.LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.stream().map(r -> r.brokerId).collect(Collectors.toList()), zkVersion);
        // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
        Pair<Boolean,Integer>  pair = ZkUtils.conditionalUpdatePersistentPath(zkClient,
                ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId),
                ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch), zkVersion);
        if (pair.getKey()){
            inSyncReplicas = newIsr;
            zkVersion = pair.getValue();
            logger.trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.toString(), zkVersion));
        } else {
            logger.info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion+""));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition other = (Partition) o;
        return topic.equals(other.topic) && partitionId == other.partitionId;
    }

    @Override
    public int hashCode() {
        return 31 + topic.hashCode() + 17*partitionId;
    }
}

package kafka.controller;

import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. NonExistentReplica: If a replica is deleted, it is moved to this state. Valid previous state is OfflineReplica
 */
public class ReplicaStateMachine {

    private static Logger logger = Logger.getLogger(ReplicaStateMachine.class);

    KafkaController controller;

    public ReplicaStateMachine(KafkaController controller) {
        this.controller = controller;
        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;

        brokerRequestBatch = new ControllerChannelManager.ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
                controllerId, controller.clientId);

        noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);
    }

    private ControllerContext controllerContext ;
    private int controllerId ;
    private ZkClient zkClient ;
    Map<TopicAndPartition, ReplicaState> replicaState = new HashMap<>();
    ControllerChannelManager.ControllerBrokerRequestBatch brokerRequestBatch ;
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    private NoOpLeaderSelector noOpPartitionLeaderSelector;


    /**
     * Invoked on successful controller election. First registers a broker change listener since that triggers all
     * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
     * Then triggers the OnlineReplica state change for all replicas.
     */
    public void startup() {
        // initialize replica state
        initializeReplicaState();
        hasStarted.set(true);
        // move all Online replicas to Online
        handleStateChanges(getAllReplicasOnBroker(controllerContext.allTopics.toSeq,
                controllerContext.liveBrokerIds.toSeq), OnlineReplica);
        logger.info("Started replica state machine with initial state -> " + replicaState.toString());
    }

    // register broker change listener
    public void registerListeners() {
        registerBrokerChangeListener();
    }

    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        hasStarted.set(false);
        replicaState.clear();
    }

    /**
     * This API is invoked by the broker change controller callbacks and the startup API of the state machine
     * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
     * @param targetState  The state that the replicas should be moved to
     * The controller's allLeaders cache should have been updated before this
     */
    public void handleStateChanges(Set<KafkaController.PartitionAndReplica> replicas, ReplicaState targetState) {
        info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
        try {
            brokerRequestBatch.newBatch()
            replicas.foreach(r => handleStateChange(r.topic, r.partition, r.replica, targetState))
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
        }catch {
            case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
        }
    }

    /**
     * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state.
     * @param topic       The topic of the replica for which the state transition is invoked
     * @param partition   The partition of the replica for which the state transition is invoked
     * @param replicaId   The replica for which the state transition is invoked
     * @param targetState The end state that the replica should be moved to
     */
    public void handleStateChange(String topic, int partition, int replicaId, ReplicaState targetState) {
        val topicAndPartition = TopicAndPartition(topic, partition)
        if (!hasStarted.get)
            throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                    "to %s failed because replica state machine has not started")
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
        try {
            replicaState.getOrElseUpdate((topic, partition, replicaId), NonExistentReplica)
            val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
            targetState match {
                case NewReplica =>
                    assertValidPreviousStates(topic, partition, replicaId, List(NonExistentReplica), targetState)
                    // start replica as a follower to the current leader for its partition
                    val leaderIsrAndControllerEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
                    leaderIsrAndControllerEpochOpt match {
                    case Some(leaderIsrAndControllerEpoch) =>
                        if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                            throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                                    .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
                        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                topic, partition, leaderIsrAndControllerEpoch,
                                replicaAssignment)
                    case None => // new leader request will be sent to this replica when one gets elected
                }
                replicaState.put((topic, partition, replicaId), NewReplica)
                stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NewReplica"
                        .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                case NonExistentReplica =>
                    assertValidPreviousStates(topic, partition, replicaId, List(OfflineReplica), targetState)
                    // send stop replica command
                    brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true)
                    // remove this replica from the assigned replicas list for its partition
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
                    replicaState.remove((topic, partition, replicaId))
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NonExistentReplica"
                            .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                case OnlineReplica =>
                    assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica, OfflineReplica), targetState)
                    replicaState((topic, partition, replicaId)) match {
                    case NewReplica =>
                        // add this replica to the assigned replicas list for its partition
                        val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                        if(!currentAssignedReplicas.contains(replicaId))
                            controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
                        stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                    case _ =>
                        // check if the leader for this partition ever existed
                        controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                        case Some(leaderIsrAndControllerEpoch) =>
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                                    replicaAssignment)
                            replicaState.put((topic, partition, replicaId), OnlineReplica)
                            stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                        case None => // that means the partition was never in OnlinePartition state, this means the broker never
                            // started a log for that partition and does not have a high watermark value for this partition
                    }

                }
                replicaState.put((topic, partition, replicaId), OnlineReplica)
                case OfflineReplica =>
                    assertValidPreviousStates(topic, partition, replicaId, List(NewReplica, OnlineReplica), targetState)
                    // As an optimization, the controller removes dead replicas from the ISR
                    val leaderAndIsrIsEmpty: Boolean =
                        controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                    case Some(currLeaderIsrAndControllerEpoch) =>
                        if (currLeaderIsrAndControllerEpoch.leaderAndIsr.isr.contains(replicaId))
                            controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                        case Some(updatedLeaderIsrAndControllerEpoch) =>
                            // send the shrunk ISR state change request only to the leader
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader),
                                    topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                            replicaState.put((topic, partition, replicaId), OfflineReplica)
                            stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                            false
                        case None =>
                            true
                    }
                else {
                        replicaState.put((topic, partition, replicaId), OfflineReplica)
                        stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                                .format(controllerId, controller.epoch, replicaId, topicAndPartition))
                        false
                    }
                    case None =>
                        true
                }
                if (leaderAndIsrIsEmpty)
                    throw new StateChangeFailedException(
                            "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
                                    .format(replicaId, topicAndPartition))
            }
        }
        catch {
            case t: Throwable =>
                stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] to %s failed"
                        .format(controllerId, controller.epoch, replicaId, topic, partition, targetState), t)
        }
    }

    private void assertValidPreviousStates(String topic, int partition, int replicaId, List<ReplicaState> fromStates,
                                           ReplicaState targetState) {
        assert(fromStates.contains(replicaState((topic, partition, replicaId))),
        "Replica %s for partition [%s,%d] should be in the %s states before moving to %s state"
                .format(replicaId, topic, partition, fromStates.mkString(","), targetState) +
                ". Instead it is in %s state".format(replicaState((topic, partition, replicaId))))
    }

    private void registerBrokerChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener());
    }

    /**
     * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
     * in zookeeper
     */
    private void initializeReplicaState() {
        for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
            val topic = topicPartition.topic
            val partition = topicPartition.partition
            assignedReplicas.foreach { replicaId =>
                controllerContext.liveBrokerIds.contains(replicaId) match {
                    case true => replicaState.put((topic, partition, replicaId), OnlineReplica)
                    case false => replicaState.put((topic, partition, replicaId), OfflineReplica)
                }
            }
        }
    }

    private Set<PartitionAndReplica> getAllReplicasOnBroker(List<String> topics, List<Integer> brokerIds) {
        brokerIds.map { brokerId =>
            val partitionsAssignedToThisBroker =
                    controllerContext.partitionReplicaAssignment.filter(p => topics.contains(p._1.topic) && p._2.contains(brokerId))
            if(partitionsAssignedToThisBroker.size == 0)
                info("No state transitions triggered since no partitions are assigned to brokers %s".format(brokerIds.mkString(",")))
            partitionsAssignedToThisBroker.map(p => new PartitionAndReplica(p._1.topic, p._1.partition, brokerId))
        }.flatten.toSet
    }

    public List<TopicAndPartition> getPartitionsAssignedToBroker(List<String> topics,int brokerId){
        controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
    }


    public  class BrokerChangeListener implements IZkChildListener {
        public void handleChildChange(String parentPath, List<String> currentBrokerList) throws Exception{
            info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
            controllerContext.controllerLock synchronized {
                if (hasStarted.get) {
                    ControllerStats.leaderElectionTimer.time {
                        try {
                            val curBrokerIds = currentBrokerList.map(_.toInt).toSet
                            val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds
                            val newBrokerInfo = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _))
                            val newBrokers = newBrokerInfo.filter(_.isDefined).map(_.get)
                            val deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds -- curBrokerIds
                            controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
                            info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                                    .format(newBrokerIds.mkString(","), deadBrokerIds.mkString(","), controllerContext.liveBrokerIds.mkString(",")))
                            newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
                            deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))
                            if(newBrokerIds.size > 0)
                                controller.onBrokerStartup(newBrokerIds.toSeq)
                            if(deadBrokerIds.size > 0)
                                controller.onBrokerFailure(deadBrokerIds.toSeq)
                        } catch {
                            case e: Throwable => error("Error while handling broker changes", e)
                        }
                    }
                }
            }
        }
    }
    public static interface ReplicaState { byte state(); }
    public  static class  NewReplica implements ReplicaState { public byte state(){return 0;} }
    public  static class  OnlineReplica implements ReplicaState { public byte state(){return 1;}}
    public  static class  OfflineReplica implements ReplicaState{ public byte state(){return 2;} }
    public  static class  NonExistentReplica implements ReplicaState { public byte state(){return 3;} }
}

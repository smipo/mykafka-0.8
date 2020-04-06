package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
public class PartitionStateMachine {

    private static Logger logger = Logger.getLogger(PartitionStateMachine.class);

    KafkaController controller;

    public PartitionStateMachine(KafkaController controller) {
        this.controller = controller;
        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;

        brokerRequestBatch = new ControllerChannelManager.ControllerBrokerRequestBatch(controller.controllerContext, controller.sendRequest,
                controllerId, controller.clientId);

        noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);
    }

    public ControllerContext controllerContext ;
    public int controllerId ;
    public ZkClient zkClient ;
    public Map<TopicAndPartition, PartitionState> partitionState = new HashMap<>();
    public ControllerChannelManager.ControllerBrokerRequestBatch brokerRequestBatch ;
    public AtomicBoolean hasStarted = new AtomicBoolean(false);
    public NoOpLeaderSelector noOpPartitionLeaderSelector;


    /**
     * Invoked on successful controller election. First registers a topic change listener since that triggers all
     * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
     * the OnlinePartition state change for all new or offline partitions.
     */
    public void startup() {
        // initialize partition state
        initializePartitionState();
        hasStarted.set(true);
        // try to move partitions to online state
        triggerOnlinePartitionStateChange();
        logger.info("Started partition state machine with initial state -> " + partitionState.toString());
    }

    // register topic and partition change listeners
    public void registerListeners() {
        registerTopicChangeListener();
    }

    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        hasStarted.set(false);
        partitionState.clear();
    }

    /**
     * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
     * state. This is called on a successful controller election and on broker changes
     */
    public void triggerOnlinePartitionStateChange() {
        try {
            brokerRequestBatch.newBatch();
            // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state
            for((topicAndPartition, partitionState) <- partitionState) {
                if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
                    handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector)
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
        } catch {
            case e: Throwable => error("Error while moving some partitions to the online state", e)
                // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
        }
    }

    /**
     * This API is invoked by the partition change zookeeper listener
     * @param partitions   The list of partitions that need to be transitioned to the target state
     * @param targetState  The state that the partitions should be moved to
     */
    public void handleStateChanges(Set<TopicAndPartition> partitions, PartitionState targetState,
                                   PartitionLeaderSelector  leaderSelector) {
        logger.info("Invoking state change to %s for partitions %s".format(targetState.toString(), partitions.toString()));
        try {
            brokerRequestBatch.newBatch()
            partitions.foreach { topicAndPartition =>
                handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector)
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
        }catch {
            case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
                // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
        }
    }

    /**
     * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state.
     * @param topic       The topic of the partition for which the state transition is invoked
     * @param partition   The partition for which the state transition is invoked
     * @param targetState The end state that the partition should be moved to
     */
    private void handleStateChange(String topic, int partition, PartitionState targetState,
                                   PartitionLeaderSelector leaderSelector) {
        val topicAndPartition = TopicAndPartition(topic, partition)
        if (!hasStarted.get)
            throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                    "the partition state machine has not started")
                    .format(controllerId, controller.epoch, topicAndPartition, targetState))
        val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
        try {
            targetState match {
                case NewPartition =>
                    // pre: partition did not exist before this
                    assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
                    assignReplicasToPartitions(topic, partition)
                    partitionState.put(topicAndPartition, NewPartition)
                    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from NotExists to New with assigned replicas %s"
                            .format(controllerId, controller.epoch, topicAndPartition, assignedReplicas))
                    // post: partition has been assigned replicas
                case OnlinePartition =>
                    assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
                    partitionState(topicAndPartition) match {
                    case NewPartition =>
                        // initialize leader and isr path for new partition
                        initializeLeaderAndIsrForPartition(topicAndPartition)
                    case OfflinePartition =>
                        electLeaderForPartition(topic, partition, leaderSelector)
                    case OnlinePartition => // invoked when the leader needs to be re-elected
                        electLeaderForPartition(topic, partition, leaderSelector)
                    case _ => // should never come here since illegal previous states are checked above
                }
                partitionState.put(topicAndPartition, OnlinePartition)
                val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
                stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to OnlinePartition with leader %d"
                        .format(controllerId, controller.epoch, topicAndPartition, partitionState(topicAndPartition), leader))
                // post: partition has a leader
                case OfflinePartition =>
                    // pre: partition should be in New or Online state
                    assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition), OfflinePartition)
                    // should be called when the leader for a partition is no longer alive
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from Online to Offline"
                            .format(controllerId, controller.epoch, topicAndPartition))
                    partitionState.put(topicAndPartition, OfflinePartition)
                    // post: partition has no alive leader
                case NonExistentPartition =>
                    // pre: partition should be in Offline state
                    assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from Offline to NotExists"
                            .format(controllerId, controller.epoch, topicAndPartition))
                    partitionState.put(topicAndPartition, NonExistentPartition)
                    // post: partition state is deleted from all brokers and zookeeper
            }
        } catch {
            case t: Throwable =>
                stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
                        .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
        }
    }

    /**
     * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
     * zookeeper
     */
    private void initializePartitionState() {
        for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
            // check if leader and isr path exists for partition. If not, then it is in NEW state
            controllerContext.partitionLeadershipInfo.get(topicPartition) match {
                case Some(currentLeaderIsrAndEpoch) =>
                    // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
                    controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
                    case true => // leader is alive
                        partitionState.put(topicPartition, OnlinePartition)
                    case false =>
                        partitionState.put(topicPartition, OfflinePartition)
                }
                case None =>
                    partitionState.put(topicPartition, NewPartition)
            }
        }
    }

    private void assertValidPreviousStates(TopicAndPartition topicAndPartition, List<PartitionState> fromStates,
                                           PartitionState targetState) {
        if(!fromStates.contains(partitionState(topicAndPartition)))
            throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
                    .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
                    .format(partitionState(topicAndPartition)))
    }

    /**
     * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
     * partition's replica assignment.
     * @param topic     The topic of the partition whose replica assignment is to be cached
     * @param partition The partition whose replica assignment is to be cached
     */
    private void assignReplicasToPartitions(String topic,int partition) {
        val assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition)
        controllerContext.partitionReplicaAssignment += TopicAndPartition(topic, partition) -> assignedReplicas
    }

    /**
     * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
     * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
     * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
     * OfflinePartition state.
     * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
     */
    private void initializeLeaderAndIsrForPartition(TopicAndPartition topicAndPartition) {
        val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
        val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
        liveAssignedReplicas.size match {
            case 0 =>
                val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                        "live brokers are [%s]. No assigned replica is alive.")
                        .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
                stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                throw new StateChangeFailedException(failMsg)
            case _ =>
                debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
                // make the first replica in the list of assigned replicas, the leader
                val leader = liveAssignedReplicas.head
                val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
                        controller.epoch)
                debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
                try {
                    ZkUtils.createPersistentPath(controllerContext.zkClient,
                            ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                            ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
                    // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
                    // took over and initialized this partition. This can happen if the current controller went into a long
                    // GC pause
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
                            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
                } catch {
                case e: ZkNodeExistsException =>
                    // read the controller epoch
                    val leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic,
                            topicAndPartition.partition).get
                    val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                            "exists with value %s and controller epoch %d")
                            .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
                    stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                    throw new StateChangeFailedException(failMsg)
            }
        }
    }

    /**
     * Invoked on the OfflinePartition->OnlinePartition state change. It invokes the leader election API to elect a leader
     * for the input offline partition
     * @param topic               The topic of the offline partition
     * @param partition           The offline partition
     * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
     */
    public void electLeaderForPartition(String topic, int partition, PartitionLeaderSelector leaderSelector) {
        val topicAndPartition = TopicAndPartition(topic, partition)
        // handle leader election for the partitions whose leader is no longer alive
        stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                .format(controllerId, controller.epoch, topicAndPartition))
        try {
            var zookeeperPathUpdateSucceeded: Boolean = false
            var newLeaderAndIsr: LeaderAndIsr = null
            var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
            while(!zookeeperPathUpdateSucceeded) {
                val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
                val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
                val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
                if (controllerEpoch > controller.epoch) {
                    val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                            "already written by another controller. This probably means that the current controller %d went through " +
                            "a soft failure and another controller was elected with epoch %d.")
                            .format(topic, partition, controllerId, controllerEpoch)
                    stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                    throw new StateChangeFailedException(failMsg)
                }
                // elect new leader or throw exception
                val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
                val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
                        ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                        ZkUtils.leaderAndIsrZkData(leaderAndIsr, controller.epoch), currentLeaderAndIsr.zkVersion)
                newLeaderAndIsr = leaderAndIsr
                newLeaderAndIsr.zkVersion = newVersion
                zookeeperPathUpdateSucceeded = updateSucceeded
                replicasForThisPartition = replicas
            }
            val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
            // update the leader cache
            controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
            stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                    .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
            val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
            // store new leader and isr info in cache
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
                    newLeaderIsrAndControllerEpoch, replicas)
        } catch {
            case lenne: LeaderElectionNotNeededException => // swallow
            case nroe: NoReplicaOnlineException => throw nroe
            case sce: Throwable =>
                val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
                stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                throw new StateChangeFailedException(failMsg, sce)
        }
        debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
    }

    public void registerTopicChangeListener()  {
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener());
    }

    public void  registerPartitionChangeListener(String topic)  {
        zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic));
    }

    private LeaderIsrAndControllerEpoch getLeaderIsrAndEpochOrThrowException(String topic, int partition) {
        val topicAndPartition = TopicAndPartition(topic, partition)
        ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition) match {
            case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
            case None =>
                val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
                throw new StateChangeFailedException(failMsg)
        }
    }

    public  class TopicChangeListener implements IZkChildListener {
        public void handleChildChange(String parentPath, List<String> children) throws Exception{
            synchronized(controllerContext.controllerLock) {
                if (hasStarted.get()) {
                    try {
                        val currentChildren = {
              import JavaConversions._
                        debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
                        (children: Buffer[String]).toSet
            }
                        val newTopics = currentChildren -- controllerContext.allTopics
                        val deletedTopics = controllerContext.allTopics -- currentChildren
                        //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
                        controllerContext.allTopics = currentChildren

                        val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.toSeq)
                        controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
                                !deletedTopics.contains(p._1.topic))
                        controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
                                info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
                                        deletedTopics, addedPartitionReplicaAssignment))
                        if(newTopics.size > 0)
                            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
                    } catch {
                        case e: Throwable => error("Error while handling new topic", e )
                    }
                    // TODO: kafka-330  Handle deleted topics
                }
            }
        }
    }

    public  class AddPartitionsListener implements IZkDataListener {
        public String topic;

        public AddPartitionsListener(String topic) {
            this.topic = topic;
        }

        public void handleDataChange(String dataPath, Object data) throws Exception{
            synchronized(controllerContext.controllerLock) {
                try {
                    logger.info("Add Partition triggered " + data + " for path " + dataPath);
                    Map<TopicAndPartition,List<Integer>> partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic));
                    Map<TopicAndPartition,List<Integer>>  partitionsRemainingToBeAdded = partitionReplicaAssignment.entrySet().stream().filter(p -> !controllerContext.partitionReplicaAssignment.contains(p.getKey())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                    logger.info("New partitions to be added [%s]".format(partitionsRemainingToBeAdded.toString()));
                    controller.onNewPartitionCreation(partitionsRemainingToBeAdded.keySet());
                } catch (Throwable e){
                    logger.error("Error while handling add partitions for data path " + dataPath, e );
                }
            }
        }

        public void handleDataDeleted(String var1) throws Exception{

        }
    }

    public static interface PartitionState { byte state(); }
    public static  class  NewPartition implements PartitionState { public byte state(){return 0;} }
    public static  class  OnlinePartition implements PartitionState { public byte state(){return 1;}}
    public static  class  OfflinePartition implements PartitionState { public byte state(){return 2;} }
    public  static class  NonExistentPartition implements PartitionState { public byte state(){return 3;} }


}

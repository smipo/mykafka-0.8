package kafka.controller;

import kafka.cluster.Broker;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Preconditions;
import kafka.utils.Three;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

        brokerRequestBatch = controllerContext.controllerChannelManager.new ControllerBrokerRequestBatch(controller.controllerContext,
                controllerId, controller.clientId());

        noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);
    }

    private ControllerContext controllerContext ;
    private int controllerId ;
    private ZkClient zkClient ;
    Map<Three<String,Integer,Integer>, ReplicaState> replicaState = new HashMap<>();
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
        handleStateChanges(getAllReplicasOnBroker(controllerContext.allTopics.stream().collect(Collectors.toList()),
                controllerContext.liveBrokerIds().stream().collect(Collectors.toList())), new OnlineReplica());
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
        logger.info("Invoking state change to %s for replicas %s".format(targetState.toString(), replicas.toString()));
        try {
            brokerRequestBatch.newBatch();
            for(KafkaController.PartitionAndReplica r:replicas){
                handleStateChange(r.topic, r.partition, r.replica, targetState);
                brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
            }
        }catch (Throwable e){
            logger.error("Error while moving some replicas to %s state".format(targetState.toString()), e);
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
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                    "to %s failed because replica state machine has not started")
                    .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition, targetState));
        try {
            replicaState.putIfAbsent(new Three(topic, partition, replicaId), new NonExistentReplica());
            List<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            if(targetState instanceof NewReplica){
                List<ReplicaState> list = new ArrayList<>();
                list.add(new NonExistentReplica());
                assertValidPreviousStates(topic, partition, replicaId, list, targetState);
                // start replica as a follower to the current leader for its partition
                LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
                if(leaderIsrAndControllerEpoch != null){
                    if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                        throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                                .format(replicaId+"", topicAndPartition) + "state as it is being requested to become leader");
                    List<Integer> brokerIds = new ArrayList<>();
                    brokerIds.add(replicaId);
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(brokerIds,
                            topic, partition, leaderIsrAndControllerEpoch,
                            replicaAssignment);
                }
                replicaState.put(new Three<>(topic, partition, replicaId),new NewReplica());
                logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NewReplica"
                        .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
            }else if(targetState instanceof NonExistentReplica){
                List<ReplicaState> list = new ArrayList<>();
                list.add(new OfflineReplica());
                assertValidPreviousStates(topic, partition, replicaId, list, targetState);
                // send stop replica command
                List<Integer> brokerIds = new ArrayList<>();
                brokerIds.add(replicaId);
                brokerRequestBatch.addStopReplicaRequestForBrokers(brokerIds, topic, partition,  true);
                // remove this replica from the assigned replicas list for its partition
                List<Integer> currentAssignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.stream().filter(r -> r != replicaId).collect(Collectors.toList()));
                replicaState.remove(new Three<>(topic, partition, replicaId));
                logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to NonExistentReplica"
                        .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
            }else if(targetState instanceof OnlineReplica){
                List<ReplicaState> list = new ArrayList<>();
                list.add(new NewReplica());
                list.add(new OnlineReplica());
                list.add(new OfflineReplica());
                assertValidPreviousStates(topic, partition, replicaId, list, targetState);
                ReplicaState replicaState1 = replicaState.get(new Three<>(topic, partition, replicaId));
                if(replicaState1 instanceof NewReplica){
                    // add this replica to the assigned replicas list for its partition
                    List<Integer> currentAssignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                    if(!currentAssignedReplicas.contains(replicaId)) {
                        currentAssignedReplicas.add(replicaId);
                        controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas);
                    }
                    logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                            .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
                }else {
                    // check if the leader for this partition ever existed
                    LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                    if(leaderIsrAndControllerEpoch != null){
                        List<Integer> brokerIds = new ArrayList<>();
                        brokerIds.add(replicaId);
                        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(brokerIds, topic, partition, leaderIsrAndControllerEpoch,
                                replicaAssignment);
                        replicaState.put(new Three<>(topic, partition, replicaId), new OnlineReplica());
                        logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OnlineReplica"
                                .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
                    }
                    replicaState.put(new Three<>(topic, partition, replicaId), new OnlineReplica());
                }
            }else if(targetState instanceof OfflineReplica){
                List<ReplicaState> list = new ArrayList<>();
                list.add(new NewReplica());
                list.add(new OnlineReplica());
                assertValidPreviousStates(topic, partition, replicaId, list, targetState);
                // As an optimization, the controller removes dead replicas from the ISR
                boolean leaderAndIsrIsEmpty = true;
                LeaderIsrAndControllerEpoch  currLeaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition) ;
                if(currLeaderIsrAndControllerEpoch != null){
                    if (currLeaderIsrAndControllerEpoch.leaderAndIsr.isr.contains(replicaId)){
                        LeaderIsrAndControllerEpoch updatedLeaderIsrAndControllerEpoch = controller.removeReplicaFromIsr(topic, partition, replicaId);
                        if(updatedLeaderIsrAndControllerEpoch != null){
                            // send the shrunk ISR state change request only to the leader
                            List<Integer> brokerIds = new ArrayList<>();
                            brokerIds.add(updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader);
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(brokerIds,
                                    topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment);
                            replicaState.put(new Three<>(topic, partition, replicaId),new OfflineReplica());
                            logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                                    .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
                            leaderAndIsrIsEmpty = false;
                        }else{
                            leaderAndIsrIsEmpty = true;
                        }
                    }else {
                        replicaState.put(new Three<>(topic, partition, replicaId), new OfflineReplica());
                        logger.trace("Controller %d epoch %d changed state of replica %d for partition %s to OfflineReplica"
                                .format(controllerId+"", controller.epoch(), replicaId, topicAndPartition));
                        leaderAndIsrIsEmpty = false;
                    }
                }
                if (leaderAndIsrIsEmpty)
                    throw new StateChangeFailedException(
                            "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
                                    .format(replicaId+"", topicAndPartition));
            }
        }
        catch(Throwable t) {
            logger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] to %s failed"
                    .format(controllerId+"", controller.epoch(), replicaId, topic, partition, targetState), t);

        }
    }

    private void assertValidPreviousStates(String topic, int partition, int replicaId, List<ReplicaState> fromStates,
                                           ReplicaState targetState) {
        Preconditions.checkArgument(fromStates.contains(replicaState.get(new Three<>(topic, partition, replicaId))),
        "Replica %s for partition [%s,%d] should be in the %s states before moving to %s state"
                .format(replicaId+"", topic, partition, fromStates.toString(), targetState) +
                ". Instead it is in %s state".format(replicaState.get(new Three(topic, partition, replicaId)).toString()));
    }

    private void registerBrokerChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener());
    }

    /**
     * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
     * in zookeeper
     */
    private void initializeReplicaState() {
        for(Map.Entry<TopicAndPartition, List<Integer>> entry : controllerContext.partitionReplicaAssignment.entrySet()){
            TopicAndPartition topicPartition  = entry.getKey();
            List<Integer> assignedReplicas = entry.getValue();
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            for(Integer replicaId:assignedReplicas){
                if(controllerContext.liveBrokerIds().contains(replicaId)){
                    replicaState.put(new Three(topic, partition, replicaId), new OnlineReplica());
                }else{
                    replicaState.put(new Three(topic, partition, replicaId), new OfflineReplica());
                }
            }
        }
    }

    private Set<KafkaController.PartitionAndReplica> getAllReplicasOnBroker(List<String> topics, List<Integer> brokerIds) {
        Set<KafkaController.PartitionAndReplica> set = new HashSet<>();
        for(Integer brokerId:brokerIds){
            Map<TopicAndPartition, List<Integer>> partitionsAssignedToThisBroker = new HashMap<>();
            for(Map.Entry<TopicAndPartition, List<Integer>> entry : controllerContext.partitionReplicaAssignment.entrySet()){
                if(!(topics.contains(entry.getKey().topic()) && entry.getValue().contains(brokerId))){
                    partitionsAssignedToThisBroker.put(entry.getKey(),entry.getValue());
                    set.add(new KafkaController.PartitionAndReplica(entry.getKey().topic(), entry.getKey().partition(), brokerId));
                }
            }
        }
        return set;
    }

    public List<TopicAndPartition> getPartitionsAssignedToBroker(List<String> topics,int brokerId){
        List<TopicAndPartition> list = new ArrayList<>();
        for(Map.Entry<TopicAndPartition, List<Integer>> entry : controllerContext.partitionReplicaAssignment.entrySet()){
            if(entry.getValue().contains(brokerId)){
                list.add(entry.getKey());
            }
        }
        return list;
    }


    public  class BrokerChangeListener implements IZkChildListener {
        public void handleChildChange(String parentPath, List<String> currentBrokerList) throws Exception{
            logger.info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.toString()));
            synchronized(controllerContext.controllerLock ) {
                if (hasStarted.get()) {
                    try {
                        Set<Integer> curBrokerIds = currentBrokerList.stream().map(x->Integer.parseInt(x)).collect(Collectors.toSet());
                        curBrokerIds.removeAll(controllerContext.liveOrShuttingDownBrokerIds());
                        Set<Integer> newBrokerIds = curBrokerIds;
                        List<Broker> newBrokerInfo = newBrokerIds.stream().map(brokerId->ZkUtils.getBrokerInfo(zkClient, brokerId)).collect(Collectors.toList());
                        List<Broker> newBrokers = newBrokerInfo.stream().filter(b->b == null).collect(Collectors.toList());
                        controllerContext.liveOrShuttingDownBrokerIds().removeAll(curBrokerIds);
                        Set<Integer> deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds() ;
                        controllerContext.liveBrokersUnderlying = curBrokerIds.stream().map(x->ZkUtils.getBrokerInfo(zkClient, x)).filter(x->x == null).collect(Collectors.toSet());
                        logger.info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                                .format(newBrokerIds.toString(), deadBrokerIds.toString(), controllerContext.liveBrokerIds().toString()));
                        for(Broker b:newBrokers){
                            controllerContext.controllerChannelManager.addBroker(b);
                        }
                        deadBrokerIds.forEach(b->controllerContext.controllerChannelManager.removeBroker(b));
                        if(newBrokerIds.size() > 0)
                            controller.onBrokerStartup(newBrokerIds.stream().collect(Collectors.toList()));
                        if(deadBrokerIds.size() > 0)
                            controller.onBrokerFailure(deadBrokerIds.stream().collect(Collectors.toList()));
                    } catch(Throwable e) {
                        logger.error("Error while handling broker changes", e);
                    }
                }
            }
        }
    }
    public static interface ReplicaState { byte state(); }
    public  static class  NewReplica implements ReplicaState { public byte state(){return 0;}
        @Override
        public int hashCode() {
            return state();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null){
                return false;
            }
            if(obj == this){
                return true;
            }
            if(obj instanceof NewReplica){
                NewReplica o1 = (NewReplica)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public  static class  OnlineReplica implements ReplicaState { public byte state(){return 1;}
        @Override
        public int hashCode() {
            return state();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null){
                return false;
            }
            if(obj == this){
                return true;
            }
            if(obj instanceof OnlineReplica){
                OnlineReplica o1 = (OnlineReplica)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public  static class  OfflineReplica implements ReplicaState{ public byte state(){return 2;}
        @Override
        public int hashCode() {
            return state();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null){
                return false;
            }
            if(obj == this){
                return true;
            }
            if(obj instanceof OfflineReplica){
                OfflineReplica o1 = (OfflineReplica)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public  static class  NonExistentReplica implements ReplicaState { public byte state(){return 3;}
        @Override
        public int hashCode() {
            return state();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null){
                return false;
            }
            if(obj == this){
                return true;
            }
            if(obj instanceof NonExistentReplica){
                NonExistentReplica o1 = (NonExistentReplica)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
}

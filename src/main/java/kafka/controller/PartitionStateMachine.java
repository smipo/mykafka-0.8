package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.NoReplicaOnlineException;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
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

        brokerRequestBatch = new ControllerChannelManager.ControllerBrokerRequestBatch(controller,controller.controllerContext,
                controllerId, controller.clientId());

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
            for(Map.Entry<TopicAndPartition, PartitionState> entry : partitionState.entrySet()){
                if(entry.getValue() instanceof OfflinePartition || entry.getValue() instanceof NewPartition)
                    handleStateChange(entry.getKey().topic(), entry.getKey().partition(), new OnlinePartition(), controller.offlinePartitionSelector);
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e){
            logger.error("Error while moving some partitions to the online state", e);
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
        logger.info(String.format("Invoking state change to %s for partitions %s",targetState.toString(), partitions.toString()));
        try {
            brokerRequestBatch.newBatch();
            for(TopicAndPartition topicAndPartition:partitions){
                handleStateChange(topicAndPartition.topic(), topicAndPartition.partition(), targetState, leaderSelector);
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        }catch (Throwable e){
            logger.error(String.format("Error while moving some partitions to %s state",targetState.toString()), e);
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
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException(String
                    .format("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                            "the partition state machine has not started",controllerId, controller.epoch(), topicAndPartition, targetState));
        PartitionState currState = partitionState.putIfAbsent(topicAndPartition, new NonExistentPartition());
        try {
            if(targetState instanceof NewPartition){
                // pre: partition did not exist before this
                List<PartitionState> fromStates = new ArrayList<>();
                fromStates.add(new NonExistentPartition());
                assertValidPreviousStates(topicAndPartition, fromStates, new NewPartition());
                assignReplicasToPartitions(topic, partition);
                partitionState.put(topicAndPartition, new NewPartition());
                String assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition).toString();
                logger.trace(String
                        .format("Controller %d epoch %d changed partition %s state from NotExists to New with assigned replicas %s",controllerId, controller.epoch(), topicAndPartition, assignedReplicas));
                // post: partition has been assigned replicas
            }else if(targetState instanceof OnlinePartition){
                List<PartitionState> fromStates = new ArrayList<>();
                fromStates.add(new NewPartition());
                fromStates.add(new OnlinePartition());
                fromStates.add(new OfflinePartition());
                assertValidPreviousStates(topicAndPartition, fromStates, new OnlinePartition());
                PartitionState state = partitionState.get(topicAndPartition);
                if(state instanceof NewPartition){
                    // initialize leader and isr path for new partition
                    initializeLeaderAndIsrForPartition(topicAndPartition);
                }else if(state instanceof OfflinePartition){
                    electLeaderForPartition(topic, partition, leaderSelector);
                }else if(state instanceof OnlinePartition){// invoked when the leader needs to be re-elected
                    electLeaderForPartition(topic, partition, leaderSelector);
                }else{ // should never come here since illegal previous states are checked above

                }
                partitionState.put(topicAndPartition, new OnlinePartition());
                int leader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
                logger .trace(String
                        .format("Controller %d epoch %d changed partition %s from %s to OnlinePartition with leader %d",controllerId, controller.epoch(), topicAndPartition, partitionState.get(topicAndPartition).toString(), leader));
                // post: partition has no alive leader
            }else if(targetState instanceof OfflinePartition){
                // pre: partition should be in New or Online state
                List<PartitionState> fromStates = new ArrayList<>();
                fromStates.add(new NewPartition());
                fromStates.add(new OnlinePartition());
                assertValidPreviousStates(topicAndPartition, fromStates, new OfflinePartition());
                // should be called when the leader for a partition is no longer alive
                logger.trace(String
                        .format("Controller %d epoch %d changed partition %s state from Online to Offline",controllerId, controller.epoch(), topicAndPartition));
                partitionState.put(topicAndPartition, new OfflinePartition());
            }else if(targetState instanceof NonExistentPartition){
                // pre: partition should be in Offline state
                List<PartitionState> fromStates = new ArrayList<>();
                fromStates.add(new OfflinePartition());
                //fromStates.add(new NonExistentPartition());
                assertValidPreviousStates(topicAndPartition, fromStates, new NonExistentPartition());
                logger.trace(String
                        .format("Controller %d epoch %d changed partition %s state from Offline to NotExists",controllerId, controller.epoch(), topicAndPartition));
                partitionState.put(topicAndPartition, new NonExistentPartition());
                // post: partition state is deleted from all brokers and zookeeper
            }
        } catch (Throwable t){
            logger.error(String
                    .format("Controller %d epoch %d initiated state change for partition %s from %s to %s failed",controllerId, controller.epoch(), topicAndPartition, currState, targetState), t);
        }
    }

    /**
     * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
     * zookeeper
     */
    private void initializePartitionState() {
        for(Map.Entry<TopicAndPartition, List<Integer>> entry : controllerContext.partitionReplicaAssignment.entrySet()){
            // check if leader and isr path exists for partition. If not, then it is in NEW state
            LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = controllerContext.partitionLeadershipInfo.get(entry.getKey());
            if(currentLeaderIsrAndEpoch != null){
                // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
                boolean isAlive = controllerContext.liveBrokerIds().contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader);
                if(isAlive){
                    partitionState.put(entry.getKey(), new OnlinePartition());
                }else{
                    partitionState.put(entry.getKey(), new OfflinePartition());
                }

            }else {
                partitionState.put(entry.getKey(), new NewPartition());
            }
        }
    }

    private void assertValidPreviousStates(TopicAndPartition topicAndPartition, List<PartitionState> fromStates,
                                           PartitionState targetState) {
        if(!fromStates.contains(partitionState.get(topicAndPartition)))
            throw new IllegalStateException(String
                    .format("Partition %s should be in the %s states before moving to %s state",topicAndPartition.toString(), fromStates.toString(), targetState) + String
                    .format(". Instead it is in %s state",partitionState.get(topicAndPartition).toString()));
    }

    /**
     * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
     * partition's replica assignment.
     * @param topic     The topic of the partition whose replica assignment is to be cached
     * @param partition The partition whose replica assignment is to be cached
     */
    private void assignReplicasToPartitions(String topic,int partition) throws IOException {
        List<Integer> assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition);
        controllerContext.partitionReplicaAssignment.put(new TopicAndPartition(topic, partition) , assignedReplicas);
    }

    /**
     * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
     * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
     * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
     * OfflinePartition state.
     * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
     */
    private void initializeLeaderAndIsrForPartition(TopicAndPartition topicAndPartition) throws IOException {
        List<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        List<Integer> liveAssignedReplicas = replicaAssignment.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        if(liveAssignedReplicas.size() == 0){
            String failMsg = String
                    .format("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                            "live brokers are [%s]. No assigned replica is alive.",topicAndPartition.toString(), replicaAssignment.toString(), controllerContext.liveBrokerIds().toString());
            logger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch()) + failMsg);
            throw new StateChangeFailedException(failMsg);
        }else {
            logger.debug(String.format("Live assigned replicas for partition %s are: [%s]",topicAndPartition.toString(), liveAssignedReplicas.toString()));
            // make the first replica in the list of assigned replicas, the leader
            int leader = liveAssignedReplicas.get(0);
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsrRequest.LeaderAndIsr(leader, liveAssignedReplicas),
                    controller.epoch());
            logger.debug(String.format("Initializing leader and isr for partition %s to %s",topicAndPartition.toString(), leaderIsrAndControllerEpoch.toString()));
            try {
                ZkUtils.createPersistentPath(controllerContext.zkClient,
                        ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic(), topicAndPartition.partition()),
                        ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch()));
                // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
                // took over and initialized this partition. This can happen if the current controller went into a long
                // GC pause
                controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch);
                brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic(),
                        topicAndPartition.partition(), leaderIsrAndControllerEpoch, replicaAssignment);
            } catch (ZkNodeExistsException e){
                // read the controller epoch
                LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic(),
                        topicAndPartition.partition());
                String failMsg = String
                        .format("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                                "exists with value %s and controller epoch %d",topicAndPartition.toString(), leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch);
                logger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch()) + failMsg);
                throw new StateChangeFailedException(failMsg);
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
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        // handle leader election for the partitions whose leader is no longer alive
        logger.trace(String
                .format("Controller %d epoch %d started leader election for partition %s",controllerId, controller.epoch(), topicAndPartition));
        try {
            boolean zookeeperPathUpdateSucceeded = false;
            LeaderAndIsrRequest.LeaderAndIsr newLeaderAndIsr = null;
            List<Integer> replicasForThisPartition = new ArrayList<>();
            while(!zookeeperPathUpdateSucceeded) {
                LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition);
                LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr;
                int controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > controller.epoch()) {
                    String failMsg = String
                            .format("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                                    "already written by another controller. This probably means that the current controller %d went through " +
                                    "a soft failure and another controller was elected with epoch %d.",topic, partition, controllerId, controllerEpoch);
                    logger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch()) + failMsg);
                    throw new StateChangeFailedException(failMsg);
                }
                // elect new leader or throw exception
                Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> pair = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr);
                Pair<Boolean,Integer> pair2 = ZkUtils.conditionalUpdatePersistentPath(zkClient,
                        ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                        ZkUtils.leaderAndIsrZkData(pair.getKey(), controller.epoch()), currentLeaderAndIsr.zkVersion);
                newLeaderAndIsr = pair.getKey();
                newLeaderAndIsr.zkVersion = pair2.getValue();
                zookeeperPathUpdateSucceeded = pair2.getKey();
                replicasForThisPartition = pair.getValue();
            }
            LeaderIsrAndControllerEpoch newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch());
            // update the leader cache
            controllerContext.partitionLeadershipInfo.put(new TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch);
            logger.trace(String
                    .format("Controller %d epoch %d elected leader %d for Offline partition %s",controllerId, controller.epoch(), newLeaderAndIsr.leader, topicAndPartition));
            List<Integer> replicas = controllerContext.partitionReplicaAssignment.get(new TopicAndPartition(topic, partition));
            // store new leader and isr info in cache
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
                    newLeaderIsrAndControllerEpoch, replicas);
        } catch(LeaderElectionNotNeededException lenne) {
            logger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch()) + lenne);
        }catch (NoReplicaOnlineException nroe){
            throw nroe;
        }catch (Throwable sce){
            String failMsg = String.format("encountered error while electing leader for partition %s due to: %s.",topicAndPartition.toString(), sce.getMessage());
            logger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch()) + failMsg);
            throw new StateChangeFailedException(failMsg, sce);
        }
        logger.debug(String.format("After leader election, leader cache is updated to %s",controllerContext.partitionLeadershipInfo.toString()));
    }

    public void registerTopicChangeListener()  {
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener());
    }

    public void  registerPartitionChangeListener(String topic)  {
        zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic));
    }

    private LeaderIsrAndControllerEpoch getLeaderIsrAndEpochOrThrowException(String topic, int partition) throws IOException {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition) ;
        if(currentLeaderIsrAndEpoch == null){
            String failMsg = String
                    .format( "LeaderAndIsr information doesn't exist for partition %s in %s state",topicAndPartition.toString(), partitionState.get(topicAndPartition).toString());
            throw new StateChangeFailedException(failMsg);
        }
        return currentLeaderIsrAndEpoch;
    }

    public  class TopicChangeListener implements IZkChildListener {
        public void handleChildChange(String parentPath, List<String> children) throws Exception{
            synchronized(controllerContext.controllerLock) {
                if (hasStarted.get()) {
                    try {
                        Set<String> currentChildren = children.stream().collect(Collectors.toSet());
                        Set<String> allChildren = new HashSet<>(currentChildren);
                        currentChildren.removeAll(controllerContext.allTopics);
                        Set<String> newTopics = currentChildren ;
                        controllerContext.allTopics.removeAll(currentChildren);
                        Set<String> deletedTopics = controllerContext.allTopics ;
                        //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
                        controllerContext.allTopics = allChildren;

                        Map<TopicAndPartition,List<Integer>> addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.stream().collect(Collectors.toList()));
                        controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.entrySet().stream().filter(p ->
                                !deletedTopics.contains(p.getKey().topic())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                        controllerContext.partitionReplicaAssignment.putAll(addedPartitionReplicaAssignment);
                        logger.info(String.format("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]",newTopics.toString(),
                                deletedTopics.toString(), addedPartitionReplicaAssignment.toString()));
                        if(newTopics.size() > 0)
                            controller.onNewTopicCreation(newTopics.stream().collect(Collectors.toList()), addedPartitionReplicaAssignment.keySet());
                    } catch (Throwable e){
                        logger.error("Error while handling new topic", e );
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
                    List<String> topics = new ArrayList<>();
                    topics.add(topic);
                    Map<TopicAndPartition,List<Integer>> partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, topics);
                    Map<TopicAndPartition,List<Integer>>  partitionsRemainingToBeAdded = partitionReplicaAssignment.entrySet().stream().filter(p -> !controllerContext.partitionReplicaAssignment.containsKey(p.getKey())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                    logger.info(String.format("New partitions to be added [%s]",partitionsRemainingToBeAdded.toString()));
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
    public static  class  NewPartition implements PartitionState { public byte state(){return 0;}

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
            if(obj instanceof NewPartition){
                NewPartition o1 = (NewPartition)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public static  class  OnlinePartition implements PartitionState { public byte state(){return 1;}

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
            if(obj instanceof OnlinePartition){
                OnlinePartition o1 = (OnlinePartition)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public static  class  OfflinePartition implements PartitionState { public byte state(){return 2;}
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
            if(obj instanceof OfflinePartition){
                OfflinePartition o1 = (OfflinePartition)obj;
                return state() == o1.state();
            }
            return false;
        }
    }
    public  static class  NonExistentPartition implements PartitionState { public byte state(){return 3;}
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
            if(obj instanceof NonExistentPartition){
                NonExistentPartition o1 = (NonExistentPartition)obj;
                return state() == o1.state();
            }
            return false;
        }
    }


}

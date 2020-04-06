package kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.api.LeaderAndIsrRequest;
import kafka.cluster.Broker;
import kafka.common.*;
import kafka.server.KafkaConfig;
import kafka.server.ZookeeperLeaderElector;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.awt.SystemColor.info;
import static org.apache.log4j.helpers.LogLog.warn;

public class KafkaController {

    public static int InitialControllerEpoch = 1;
    public static int InitialControllerEpochZkVersion = 1;

    private static Logger logger = Logger.getLogger(KafkaController.class);

    KafkaConfig config;
    ZkClient zkClient;

    public KafkaController(KafkaConfig config, ZkClient zkClient) {
        this.config = config;
        this.zkClient = zkClient;

        controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs);
        partitionStateMachine = new PartitionStateMachine(this);
        replicaStateMachine = new ReplicaStateMachine(this);
        controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath,
                config.brokerId,this);
        offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext);
        reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext);
        preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext);
        controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext);
        brokerRequestBatch = new ControllerChannelManager.ControllerBrokerRequestBatch(controllerContext, sendRequest(), this.config.brokerId, this.clientId());

        registerControllerChangedListener();
    }

    private boolean isRunning = true;
    public ControllerContext controllerContext ;

    public PartitionStateMachine partitionStateMachine ;
    public ReplicaStateMachine replicaStateMachine ;
    public ZookeeperLeaderElector controllerElector ;
    public OfflinePartitionLeaderSelector offlinePartitionSelector ;
    public ReassignedPartitionLeaderSelector reassignedPartitionLeaderSelector;
    public PreferredReplicaPartitionLeaderSelector preferredReplicaPartitionLeaderSelector ;
    public ControlledShutdownLeaderSelector controlledShutdownPartitionLeaderSelector ;
    public ControllerChannelManager.ControllerBrokerRequestBatch brokerRequestBatch ;


    public int epoch (){
        return controllerContext.epoch;
    }

    public String clientId (){
        return "id_%d-host_%s-port_%d".format(config.brokerId + "", config.hostName, config.port);
    }

    /**
     * On clean shutdown, the controller first determines the partitions that the
     * shutting down broker leads, and moves leadership of those partitions to another broker
     * that is in that partition's ISR.
     *
     * @param id Id of the broker to shutdown.
     * @return The number of partitions that the broker still leads.
     */
    public Set<TopicAndPartition> shutdownBroker(int id) throws IOException {

        if (!isActive()) {
            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown");
        }

        synchronized(controllerContext.brokerShutdownLock ) {
            logger.info("Shutting down broker " + id);

            synchronized( controllerContext.controllerLock) {
                if (!controllerContext.liveOrShuttingDownBrokerIds().contains(id))
                    throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id + ""));

                controllerContext.shuttingDownBrokerIds.add(id);

                logger.debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.toString());
                logger.debug("Live brokers: " + controllerContext.liveBrokerIds().toString());
            }

            List<Pair<TopicAndPartition,Integer>> allPartitionsAndReplicationFactorOnBroker = new ArrayList<>();
            synchronized (controllerContext.controllerLock ){
                List<Pair<String,Integer>>  list = ZkUtils.getPartitionsAssignedToBroker(zkClient, controllerContext.allTopics.stream().collect(Collectors.toList()), id);
                for(Pair<String,Integer> pair:list){
                    TopicAndPartition topicAndPartition = new TopicAndPartition(pair.getKey(), pair.getValue());
                    allPartitionsAndReplicationFactorOnBroker.add(new Pair<>(topicAndPartition, controllerContext.partitionReplicaAssignment.get(topicAndPartition).size()));
                }
            }
            for(Pair<TopicAndPartition,Integer> pair:allPartitionsAndReplicationFactorOnBroker){
                TopicAndPartition topicAndPartition = pair.getKey();
                Integer replicationFactor = pair.getValue();

                // Move leadership serially to relinquish lock.
                synchronized( controllerContext.controllerLock) {
                    LeaderIsrAndControllerEpoch currLeaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                    if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
                        // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                        // notifies all affected brokers
                        Set<TopicAndPartition> partitions = new HashSet<>();
                        partitions.add(topicAndPartition);
                        partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(),
                                controlledShutdownPartitionLeaderSelector);
                    }
                    else {
                        // Stop the replica first. The state change below initiates ZK changes which should take some time
                        // before which the stop replica request should be completed (in most cases)
                        brokerRequestBatch.newBatch();
                        List<Integer> brokerIds = new ArrayList<>();
                        brokerIds.add(id);
                        brokerRequestBatch.addStopReplicaRequestForBrokers(brokerIds, topicAndPartition.topic(), topicAndPartition.partition(),  false);
                        brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());

                        // If the broker is a follower, updates the isr in ZK and notifies the current leader
                        Set<KafkaController.PartitionAndReplica> replicas = new HashSet<>();
                        replicas.add(new PartitionAndReplica(topicAndPartition.topic(),
                                topicAndPartition.partition(), id));
                        replicaStateMachine.handleStateChanges(replicas, new ReplicaStateMachine.OfflineReplica());
                    }
                }
            }
            Set<TopicAndPartition> resSet = new HashSet<>();
            synchronized(controllerContext.controllerLock) {
                logger.trace("All leaders = " + controllerContext.partitionLeadershipInfo.toString());
                for (Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch> entry : controllerContext.partitionLeadershipInfo.entrySet()) {
                    if(entry.getValue().leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment.get(entry.getKey()).size() > 1){
                        resSet.add(entry.getKey());
                    }
                }
            }
            return resSet;
        }
    }

    /**
     * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
     * It does the following things on the become-controller state change -
     * 1. Register controller epoch changed listener
     * 2. Increments the controller epoch
     * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
     *    leaders for all existing partitions.
     * 4. Starts the controller's channel manager
     * 5. Starts the replica state machine
     * 6. Starts the partition state machine
     * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
     * This ensures another controller election will be triggered and there will always be an actively serving controller
     */
    public void onControllerFailover() {
        if(isRunning) {
            logger.info("Broker %d starting become controller state transition".format(config.brokerId + ""));
            // increment the controller epoch
            incrementControllerEpoch(zkClient);
            // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
            registerReassignedPartitionsListener();
            registerPreferredReplicaElectionListener();
            partitionStateMachine.registerListeners();
            replicaStateMachine.registerListeners();
            initializeControllerContext();
            replicaStateMachine.startup();
            partitionStateMachine.startup();
            // register the partition change listeners for all existing topics on failover
            controllerContext.allTopics.forEach(topic -> partitionStateMachine.registerPartitionChangeListener(topic));
            logger.info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId + "", epoch()));
            initializeAndMaybeTriggerPartitionReassignment();
            initializeAndMaybeTriggerPreferredReplicaElection();
            /* send partition leadership info to all live brokers */
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds().stream().collect(Collectors.toList()), new HashSet<>());
        }
        else
            logger.info("Controller has been shut down, aborting startup/failover");
    }

    /**
     * Returns true if this broker is the current controller.
     */
    public boolean isActive() {
        synchronized(controllerContext.controllerLock) {
            return controllerContext.controllerChannelManager != null;
        }
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
     * brokers as input. It does the following -
     * 1. Triggers the OnlinePartition state change for all new/offline partitions
     * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
     *    so, it performs the reassignment logic for each topic/partition.
     *
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
     * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
     *    partitions currently new or offline (rather than every partition this controller is aware of)
     * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
     *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
     */
    public void onBrokerStartup(List<Integer> newBrokers) throws IOException {
        logger.info("New broker startup callback for %s".format(newBrokers.toString()));

        Set<Integer> newBrokersSet = newBrokers.stream().collect(Collectors.toSet());
        // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown
        // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the
        // metadata will reach the new brokers faster
        sendUpdateMetadataRequest(newBrokers,new HashSet<>());
        // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
        // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
        replicaStateMachine.handleStateChanges(ZkUtils.getAllReplicasOnBroker(zkClient, controllerContext.allTopics.stream().collect(Collectors.toList()), newBrokers),new ReplicaStateMachine.OnlineReplica());
        // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
        // to see if these brokers can become leaders for some/all of those
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // check if reassignment of some partitions need to be restarted
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsWithReplicasOnNewBrokers = new HashMap<>();

        for (Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry :  controllerContext.partitionsBeingReassigned.entrySet()) {
            for(int r:entry.getValue().newReplicas){
                if(newBrokersSet.contains(r)){
                    partitionsWithReplicasOnNewBrokers.put(entry.getKey(),entry.getValue());
                }
            }
        }
        partitionsWithReplicasOnNewBrokers.forEach((k,v) -> onPartitionReassignment(k, v));
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Mark partitions with dead leaders as offline
     * 2. Triggers the OnlinePartition state change for all new/offline partitions
     * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
     *
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
     * the partition state machine will refresh our cache for us when performing leader election for all new/offline
     * partitions coming online.
     */
    public void onBrokerFailure(List<Integer> deadBrokers) throws IOException {
        logger.info("Broker failure callback for %s".format(deadBrokers.toString()));

        List<Integer> deadBrokersThatWereShuttingDown =
                deadBrokers.stream().filter(id -> controllerContext.shuttingDownBrokerIds.remove(id)).collect(Collectors.toList());
        logger.info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown.toString()));

        Set<Integer> deadBrokersSet = deadBrokers.stream().collect(Collectors.toSet());
        // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
        Set<TopicAndPartition> partitionsWithoutLeader = new HashSet<>();
        for (Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch> entry : controllerContext.partitionLeadershipInfo.entrySet()) {
            if(deadBrokersSet.contains(entry.getValue().leaderAndIsr.leader)){
                partitionsWithoutLeader.add(entry.getKey());
            }
        }
        partitionStateMachine.handleStateChanges(partitionsWithoutLeader, new PartitionStateMachine.OfflinePartition(),partitionStateMachine.noOpPartitionLeaderSelector);
        // trigger OnlinePartition state changes for offline or new partitions
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // handle dead replicas
        replicaStateMachine.handleStateChanges(ZkUtils.getAllReplicasOnBroker(zkClient, controllerContext.allTopics.stream().collect(Collectors.toList()), deadBrokers), new ReplicaStateMachine.OfflineReplica());
    }

    /**
     * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Registers partition change listener. This is not required until KAFKA-347
     * 2. Invokes the new partition callback
     */
    public void onNewTopicCreation(List<String> topics,Set<TopicAndPartition> newPartitions) {
        logger.info("New topic creation callback for %s".format(newPartitions.toString()));
        // subscribe to partition changes
        topics.forEach(topic -> partitionStateMachine.registerPartitionChangeListener(topic));
        onNewPartitionCreation(newPartitions);
    }

    /**
     * This callback is invoked by the topic change callback with the list of failed brokers as input.
     * It does the following -
     * 1. Move the newly created partitions to the NewPartition state
     * 2. Move the newly created partitions from NewPartition->OnlinePartition state
     */
    public void onNewPartitionCreation(Set<TopicAndPartition> newPartitions) {
        logger.info("New partition creation callback for %s".format(newPartitions.toString()));
        partitionStateMachine.handleStateChanges(newPartitions, new PartitionStateMachine.NewPartition(),partitionStateMachine.noOpPartitionLeaderSelector);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), new ReplicaStateMachine.NewReplica());
        partitionStateMachine.handleStateChanges(newPartitions, new PartitionStateMachine.OnlinePartition(), offlinePartitionSelector);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), new PartitionStateMachine.OnlinePartition());
    }

    /**
     * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
     * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
     * Reassigning replicas for a partition goes through a few stages -
     * RAR = Reassigned replicas
     * AR = Original list of replicas for partition
     * 1. Start new replicas RAR - AR.
     * 2. Wait until new replicas are in sync with the leader
     * 3. If the leader is not in RAR, elect a new leader from RAR
     * 4. Stop old replicas AR - RAR
     * 5. Write new AR
     * 6. Remove partition from the /admin/reassign_partitions path
     */
    public void onPartitionReassignment(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        if(areReplicasInIsr(topicAndPartition.topic(), topicAndPartition.partition(), reassignedReplicas)){
            // mark the new replicas as online
            for(int replica:reassignedReplicas ){
                Set<KafkaController.PartitionAndReplica> replicas = new HashSet<>();
                replicas.add(new PartitionAndReplica(topicAndPartition.topic(), topicAndPartition.partition(),
                        replica));
                replicaStateMachine.handleStateChanges(replicas, new ReplicaStateMachine.OnlineReplica());
            }
            // check if current leader is in the new replicas list. If not, controller needs to trigger leader election
            moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext);
            // stop older replicas
            stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext);
            // write the new list of replicas for this partition in zookeeper
            updateAssignedReplicasForPartition(topicAndPartition, reassignedPartitionContext);
            // update the /admin/reassign_partitions path to remove this partition
            removePartitionFromReassignedPartitions(topicAndPartition);
            logger.info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition.toString()));
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
            // after electing leader, the replicas and isr information changes, so resend the update metadata request
            Set<TopicAndPartition> set = new HashSet<>();
            set.add(topicAndPartition);
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds().stream().collect(Collectors.toList()),set );
        }else{
            logger.info("New replicas %s for partition %s being ".format(reassignedReplicas.toString(), topicAndPartition) +
                    "reassigned not yet caught up with the leader");
            // start new replicas
            startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext);
            logger.info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.toString(), topicAndPartition) +
                    "reassigned to catch up with the leader")
        }
    }

    public void watchIsrChangesForReassignedPartition(String topic,
                                                      int partition,
                                                      ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
                reassignedReplicas.toSet)
        reassignedPartitionContext.isrChangeListener = isrChangeListener
        // register listener on the leader and isr path to wait until they catch up with the current leader
        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
    }

    public void  initiateReassignReplicasForTopicPartition(TopicAndPartition topicAndPartition,
                                                           ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> newReplicas = reassignedPartitionContext.newReplicas;
        String topic = topicAndPartition.topic();
        int partition = topicAndPartition.partition();
        List<Integer> aliveNewReplicas = newReplicas.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        try {
            List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            if(assignedReplicas != null){
                boolean isSame = true;
                for(Integer r:assignedReplicas){
                    if(!newReplicas.contains(r)){
                        isSame = false;
                        break;
                    }
                }
                if(isSame) {
                    throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition.toString()) +
                            " %s. Ignoring request for partition reassignment".format(newReplicas.toString()));
                } else {
                    boolean isSameAlive = true;
                    for(Integer r:aliveNewReplicas){
                        if(!newReplicas.contains(r)){
                            isSame = false;
                            break;
                        }
                    }
                    if(isSame) {
                        logger.info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition.toString(), newReplicas.toString()));
                        // first register ISR change listener
                        watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext);
                        controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext);
                        onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                    } else {
                        // some replica in RAR is not alive. Fail partition reassignment
                        throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.toString()) +
                                " %s for partition %s to be reassigned are alive. ".format(newReplicas.toString(), topicAndPartition) +
                                "Failing partition reassignment");
                    }
                }
            }else{
                throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
                        .format(topicAndPartition.toString()));
            }
        } catch (Throwable e){
            logger.error("Error completing reassignment of partition %s".format(topicAndPartition.toString()), e);
            // remove the partition from the admin path to unblock the admin client
            removePartitionFromReassignedPartitions(topicAndPartition);
        }
    }

    public void onPreferredReplicaElection(Set<TopicAndPartition> partitions) {
        logger.info("Starting preferred replica leader election for partitions %s".format(partitions.toString()));
        try {
            controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitions);
            partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(), preferredReplicaPartitionLeaderSelector);
        } catch(Throwable e) {
            logger. error("Error completing preferred replica leader election for partitions %s".format(partitions.toString()), e);
        } finally {
            removePartitionsFromPreferredReplicaElection(partitions);
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
     * is the controller. It merely registers the session expiration listener and starts the controller leader
     * elector
     */
    public void startup()  {
        synchronized(controllerContext.controllerLock) {
            logger.info("Controller starting up");
            registerSessionExpirationListener();
            isRunning = true;
            controllerElector.startup();
            logger.info("Controller startup complete");
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
     */
    public void shutdown()  {
        synchronized(controllerContext.controllerLock) {
            isRunning = false;
            partitionStateMachine.shutdown();
            replicaStateMachine.shutdown();
            if(controllerContext.controllerChannelManager != null) {
                controllerContext.controllerChannelManager.shutdown();
                controllerContext.controllerChannelManager = null;
                logger.info("Controller shutdown complete");
            }
        }
    }

    public void sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)
    }

    public void incrementControllerEpoch(ZkClient zkClient)  {
        try {
            int newControllerEpoch = controllerContext.epoch + 1;
            Pair<Boolean,Integer> pair  = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
                    ZkUtils.ControllerEpochPath, String.valueOf(newControllerEpoch), controllerContext.epochZkVersion);
            if(!pair.getKey())
                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure");
            else {
                controllerContext.epochZkVersion = pair.getValue();
                controllerContext.epoch = newControllerEpoch;
            }
        } catch(ZkNoNodeException nne) {
            // if path doesn't exist, this is the first controller whose epoch should be 1
            // the following call can still fail if another controller gets elected between checking if the path exists and
            // trying to create the controller epoch path
            try {
                zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch);
                controllerContext.epoch = KafkaController.InitialControllerEpoch;
                controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion;
            } catch (ZkNodeExistsException e){
                throw new ControllerMovedException("Controller moved to another broker. " +
                        "Aborting controller startup procedure");
            }catch (Throwable oe){
                logger.error("Error while incrementing controller epoch", oe);
            }
        }catch (Throwable oe){
            logger.error("Error while incrementing controller epoch", oe);
        }
        logger.info("Controller %d incremented epoch to %d".format(config.brokerId + "", controllerContext.epoch));
    }

    private void registerSessionExpirationListener()  {
        zkClient.subscribeStateChanges(new SessionExpirationListener());
    }

    private void initializeControllerContext() throws IOException {
        Set<Broker> brokerSet =  controllerContext.liveBrokers();
        brokerSet = ZkUtils.getAllBrokersInCluster(zkClient).stream().collect(Collectors.toSet());
        controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).stream().collect(Collectors.toSet());
        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.stream().collect(Collectors.toList()));
        controllerContext.partitionLeadershipInfo = new HashMap<>();
        controllerContext.shuttingDownBrokerIds = new HashSet<>();
        // update the leader and isr cache for all existing partitions from Zookeeper
        updateLeaderAndIsrCache();
        // start the channel manager
        startChannelManager();
        logger.info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds().toString()));
        logger.info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds.toString()));
        logger.info("Current list of topics in the cluster: %s".format(controllerContext.allTopics.toString()));
    }

    private void initializeAndMaybeTriggerPartitionReassignment() throws IOException {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // check if they are already completed
        List<TopicAndPartition> reassignedPartitions = new ArrayList<>();
        for (Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry : partitionsBeingReassigned.entrySet()) {
            List<Integer> list = controllerContext.partitionReplicaAssignment.get(entry.getKey());
            boolean isSame = true;
            for(Integer r:entry.getValue().newReplicas){
                if(!list.contains(r)){
                    isSame = false;
                    break;
                }
            }
            if(isSame){
                reassignedPartitions.add(entry.getKey());
            }

        }
        reassignedPartitions.forEach(p -> removePartitionFromReassignedPartitions(p));
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsToReassign = new HashMap<>();
        partitionsToReassign.putAll(partitionsBeingReassigned);
        reassignedPartitions.forEach(p -> partitionsToReassign.remove(p));

        logger.info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()));
        logger.info("Partitions already reassigned: %s".format(reassignedPartitions.toString()));
        logger.info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()));

        partitionsToReassign.forEach( (k,v) ->
            initiateReassignReplicasForTopicPartition(k, v)
        );
    }

    private void initializeAndMaybeTriggerPreferredReplicaElection() throws IOException {
        // read the partitions undergoing preferred replica election from zookeeper path
        Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient);
        // check if they are already completed
        Set<TopicAndPartition>  partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.stream().filter(partition ->
                controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader == controllerContext.partitionReplicaAssignment.get(partition).get(0)).collect(Collectors.toSet());
        controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitionsUndergoingPreferredReplicaElection);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsThatCompletedPreferredReplicaElection);
        logger.info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.toString()));
        logger.info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.toString()));
        logger.info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.toString()));
        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.stream().collect(Collectors.toSet()));
    }

    private void startChannelManager() {
        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config);
        controllerContext.controllerChannelManager.startup();
    }

    private void updateLeaderAndIsrCache() throws IOException {
        Map<TopicAndPartition, LeaderIsrAndControllerEpoch>  leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet());
        leaderAndIsrInfo.forEach((k,v)-> controllerContext.partitionLeadershipInfo.put(k, v));

    }

    private boolean areReplicasInIsr(String topic, int partition, List<Integer> replicas) throws IOException {
        LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition) ;
        if(leaderAndIsr == null) return false;
        List<Integer>  replicasNotInIsr = replicas.stream().filter(r -> !leaderAndIsr.isr.contains(r)).collect(Collectors.toList());
        return replicasNotInIsr.isEmpty();
    }

    private void moveReassignedPartitionLeaderIfRequired(TopicAndPartition topicAndPartition,
                                                         ReassignedPartitionsContext reassignedPartitionContext ) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        int currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
            logger.info("Leader %s for partition %s being reassigned, ".format(currentLeader + "", topicAndPartition.toString()) +
                    "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.toString()));
            // move the leader to one of the alive and caught up new replicas
            Set<TopicAndPartition> partitions = new HashSet<>();
            partitions.add(topicAndPartition);
            partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(), reassignedPartitionLeaderSelector);
        } else {
            // check if the leader is alive or not
           boolean isContains = controllerContext.liveBrokerIds().contains(currentLeader);
           if(isContains){
               logger.info("Leader %s for partition %s being reassigned, ".format(currentLeader+"", topicAndPartition.toString()) +
                       "is already in the new list of replicas %s and is alive".format(reassignedReplicas.toString()));
           }else{
               logger.info("Leader %s for partition %s being reassigned, ".format(currentLeader+"", topicAndPartition.toString()) +
                       "is already in the new list of replicas %s but is dead".format(reassignedReplicas.toString()));
               Set<TopicAndPartition> partitions = new HashSet<>();
               partitions.add(topicAndPartition);
               partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(), reassignedPartitionLeaderSelector);
           }
        }
    }

    private void stopOldReplicasOfReassignedPartition(TopicAndPartition topicAndPartition,
                                                      ReassignedPartitionsContext   reassignedPartitionContext) {
        List<Integer>  reassignedReplicas = reassignedPartitionContext.newReplicas;
        String topic = topicAndPartition.topic();
        int partition = topicAndPartition.partition();
        // send stop replica state change request to the old replicas
        Set<Integer> oldReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition).stream().collect(Collectors.toSet());
        oldReplicas.removeAll(reassignedReplicas.stream().collect(Collectors.toSet()));
                // first move the replica to offline state (the controller removes it from the ISR)
        for(Integer replica:oldReplicas){
            Set<PartitionAndReplica> set = new HashSet<>();
            set.add(new PartitionAndReplica(topic, partition, replica));
            replicaStateMachine.handleStateChanges(set, new ReplicaStateMachine.OfflineReplica());
            replicaStateMachine.handleStateChanges(set, new ReplicaStateMachine.NonExistentReplica());

        }
    }

    private void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition,
                                                    ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        Map<TopicAndPartition, List<Integer>> partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.entrySet().stream().filter(topic->topic.equals(topicAndPartition.topic())).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
        partitionsAndReplicasForThisTopic.put(topicAndPartition, reassignedReplicas);
        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic);
        logger.info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition + "", reassignedReplicas.toString()));
        // update the assigned replica list after a successful zookeeper write
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas);
        // stop watching the ISR changes for this partition
        zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic(), topicAndPartition.partition()),
                controllerContext.partitionsBeingReassigned.get(topicAndPartition).isrChangeListener);
    }

    private void startNewReplicasForReassignedPartition(TopicAndPartition topicAndPartition,
                                                        ReassignedPartitionsContext reassignedPartitionContext) {
        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
        // replicas list
        Set<Integer> assignedReplicaSet = controllerContext.partitionReplicaAssignment.get(topicAndPartition).stream().collect(Collectors.toSet());
        Set<Integer> reassignedReplicaSet =  reassignedPartitionContext.newReplicas.stream().collect(Collectors.toSet());
        Set<Integer> newReplicas = reassignedReplicaSet ;
        newReplicas.removeAll(assignedReplicaSet);
        for(Integer replica:newReplicas){
            Set<PartitionAndReplica> set = new HashSet<>();
            set.add(new PartitionAndReplica(topicAndPartition.topic(), topicAndPartition.partition(), replica));
            replicaStateMachine.handleStateChanges(set,new ReplicaStateMachine.NewReplica());
        }
    }

    private void registerReassignedPartitionsListener(){
        zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, new PartitionsReassignedListener(this));
    }

    private void registerPreferredReplicaElectionListener() {
        zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, new PreferredReplicaElectionListener(this));
    }

    private void registerControllerChangedListener() {
        zkClient.subscribeDataChanges(ZkUtils.ControllerEpochPath, new ControllerEpochListener(this));
    }

    public void removePartitionFromReassignedPartitions(TopicAndPartition topicAndPartition) throws IOException {
        // read the current list of reassigned partitions from zookeeper
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // remove this partition from that list
        partitionsBeingReassigned.remove(topicAndPartition);
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext>  updatedPartitionsBeingReassigned = partitionsBeingReassigned;
        // write the new list to zookeeper
        ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas));
        // update the cache
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
    }

    public void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition,
                                                   Map<TopicAndPartition, List<Integer>>  newReplicaAssignmentForTopic){
        try {
            String zkPath = ZkUtils.getTopicPath(topicAndPartition.topic());
            Map<String,List<Integer>> map = new HashMap<>();
            for (Map.Entry<TopicAndPartition, List<Integer>> entry : newReplicaAssignmentForTopic.entrySet()) {
                map.put(String.valueOf(entry.getKey().partition()),entry.getValue());
            }
            String jsonPartitionMap = ZkUtils.replicaAssignmentZkdata(map);
            ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap);
            logger.debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap));
        } catch (ZkNoNodeException e){
           throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic()));
        }catch(Throwable e2){
            throw new KafkaException(e2.toString());
        }
    }

    public void removePartitionsFromPreferredReplicaElection( Set<TopicAndPartition> partitionsToBeRemoved) {
        for(TopicAndPartition partition : partitionsToBeRemoved) {
            // check the status
            int currentLeader = controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader;
            int preferredReplica = controllerContext.partitionReplicaAssignment.get(partition).get(0);
            if(currentLeader == preferredReplica) {
                logger.info("Partition %s completed preferred replica leader election. New leader is %d".format(partition+"", preferredReplica));
            } else {
                logger.warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition+"", currentLeader));
            }
        }
        ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsToBeRemoved);
    }

    private Set<PartitionAndReplica> getAllReplicasForPartition(Set<TopicAndPartition> partitions){
        Set<PartitionAndReplica> res = new HashSet<>();
        for(TopicAndPartition p : partitions){
            List<Integer> replicas = controllerContext.partitionReplicaAssignment.get(p);
            for(Integer replica:replicas){
                res.add(new PartitionAndReplica(p.topic(), p.partition(), replica));
            }
        }
        return res;
    }

    /**
     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
     * metadata requests
     * @param brokers The brokers that the update metadata request should be sent to
     * @param partitions The partitions for which the metadata is to be sent
     */
    private void sendUpdateMetadataRequest(List<Integer> brokers, Set<TopicAndPartition> partitions) {
        brokerRequestBatch.newBatch();
        brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions);
        brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());
    }

    /**
     * Removes a given partition replica from the ISR; if it is not the current
     * leader and there are sufficient remaining replicas in ISR.
     * @param topic topic
     * @param partition partition
     * @param replicaId replica Id
     * @return the new leaderAndIsr (with the replica removed if it was present),
     *         or None if leaderAndIsr is empty.
     */
    public LeaderIsrAndControllerEpoch removeReplicaFromIsr(String topic, int partition, int replicaId) throws IOException {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        logger.debug("Removing replica %d from ISR %s for partition %s.".format(replicaId+"",
                controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.isr.toString(), topicAndPartition));
        LeaderIsrAndControllerEpoch finalLeaderIsrAndControllerEpoch = null;
        boolean zkWriteCompleteOrUnnecessary = false;
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
            if(leaderIsrAndEpoch != null){
                LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
                int controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
                if(controllerEpoch > epoch())
                    throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                            "means the current controller with epoch %d went through a soft failure and another ".format(epoch()+"") +
                            "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch+""));
                if (leaderAndIsr.isr.contains(replicaId)) {
                    // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
                    int newLeader ;
                    if(replicaId == leaderAndIsr.leader) newLeader = -1; else newLeader = leaderAndIsr.leader;
                    LeaderAndIsrRequest.LeaderAndIsr newLeaderAndIsr = new LeaderAndIsrRequest.LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                    leaderAndIsr.isr.stream().filter(b -> b != replicaId).collect(Collectors.toList()), leaderAndIsr.zkVersion + 1);
                    // update the new leadership decision in zookeeper or retry
                    val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(
                            zkClient,
                            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                            ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch()),
                            leaderAndIsr.zkVersion);
                    newLeaderAndIsr.zkVersion = newVersion;

                    finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch()));
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
                    if (updateSucceeded)
                        info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
                    updateSucceeded
                } else {
                    warn("Cannot remove replica %d from ISR of %s. Leader = %d ; ISR = %s"
                            .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
                    finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
                    true
                }
            }else{
                logger.warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId+"", topicAndPartition));
                zkWriteCompleteOrUnnecessary = true;
            }
        }
        return finalLeaderIsrAndControllerEpoch;
    }


    public static class  ReassignedPartitionsIsrChangeListener implements IZkDataListener {
        public  KafkaController controller;
        public  String topic;
        public  int partition;
        public  Set<Integer> reassignedReplicas;

        public ReassignedPartitionsIsrChangeListener(KafkaController controller, String topic, int partition, Set<Integer> reassignedReplicas) {
            this.controller = controller;
            this.topic = topic;
            this.partition = partition;
            this.reassignedReplicas = reassignedReplicas;

            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
        }

        ZkClient zkClient;
        ControllerContext controllerContext;

        public void handleDataChange(String dataPath, Object data) throws Exception{
            try {
                synchronized(controllerContext.controllerLock ) {
                    logger.debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data));
                    // check if this partition is still being reassigned or not
                    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
                    ReassignedPartitionsContext  reassignedPartitionContext = controllerContext.partitionsBeingReassigned.get(topicAndPartition);
                    if(reassignedPartitionContext != null){
                        // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
                        LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition);
                        if(leaderAndIsr != null){
                            boolean isSame = true;
                            for(Integer l : leaderAndIsr.isr){
                                if(!reassignedReplicas.contains(l)){
                                    isSame = false;
                                    break;
                                }
                            }
                            if(isSame) {
                                // resume the partition reassignment process
                                logger.info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                                        .format(leaderAndIsr.isr.size() + "", reassignedReplicas.size(), topicAndPartition.toString()) +
                                        "Resuming partition reassignment");
                                controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                            }
                        }else{
                            logger.error("Error handling reassignment of partition %s to replicas %s as it was never created"
                                    .format(topicAndPartition.toString(), reassignedReplicas.toString()));
                        }
                    }
                }
            }catch (Throwable e){
                logger.error("Error while handling partition reassignment", e);
            }
        }

        public void handleDataDeleted(String var1) throws Exception{

        }
    }

    public static class PartitionAndReplica{
        public String topic;
        public int partition;
        public int replica;

        public PartitionAndReplica(String topic, int partition, int replica) {
            this.topic = topic;
            this.partition = partition;
            this.replica = replica;
        }
    }
    public static class ReassignedPartitionsContext{

        public List<Integer> newReplicas;

        public ReassignedPartitionsIsrChangeListener isrChangeListener;

        public ReassignedPartitionsContext(List<Integer> newReplicas, ReassignedPartitionsIsrChangeListener isrChangeListener) {
            this.newReplicas = newReplicas;
            this.isrChangeListener = isrChangeListener;
        }
    }
}

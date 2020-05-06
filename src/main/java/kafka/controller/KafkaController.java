package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.common.*;
import kafka.server.KafkaConfig;
import kafka.server.ZookeeperLeaderElector;
import kafka.utils.JacksonUtils;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class KafkaController {

    public static int InitialControllerEpoch = 1;
    public static int InitialControllerEpochZkVersion = 1;

    private static Logger logger = Logger.getLogger(KafkaController.class);


    public static int parseControllerId(String controllerInfoString) {
        try {
            if(controllerInfoString== null || controllerInfoString.isEmpty()){
                throw new KafkaException(String.format("Failed to parse the controller info json [%s].",controllerInfoString));
            }
            Map<String,Object> controllerInfo = JacksonUtils.strToMap(controllerInfoString);
            return Integer.parseInt(controllerInfo.get("brokerid").toString());
        } catch (Throwable t){
                // It may be due to an incompatible controller register version
                logger.warn(String.format("Failed to parse the controller info as json. "
                        + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper",controllerInfoString));
                try {
                    return Integer.parseInt(controllerInfoString);
                } catch (Throwable t1){
                  throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t1);
            }
        }
    }

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
        brokerRequestBatch =  new ControllerChannelManager.ControllerBrokerRequestBatch(this,controllerContext,  this.config.brokerId, this.clientId());

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
        return String.format("id_%d-host_%s-port_%d",config.brokerId ,config.hostName, config.port);
    }
    public void sendRequest(int brokerId , RequestOrResponse request , Callback callback) throws InterruptedException {
        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback);
    }
    /**
     * On clean shutdown, the controller first determines the partitions that the
     * shutting down broker leads, and moves leadership of those partitions to another broker
     * that is in that partition's ISR.
     *
     * @param id Id of the broker to shutdown.
     * @return The number of partitions that the broker still leads.
     */
    public Set<TopicAndPartition> shutdownBroker(int id) throws IOException, InterruptedException {

        if (!isActive()) {
            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown");
        }

        synchronized(controllerContext.brokerShutdownLock ) {
            logger.info("Shutting down broker " + id);

            synchronized( controllerContext.controllerLock) {
                if (!controllerContext.liveOrShuttingDownBrokerIds().contains(id))
                    throw new BrokerNotAvailableException(String.format("Broker id %d does not exist.",id));

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
    public void onControllerFailover() throws IOException, InterruptedException {
        if(isRunning) {
            logger.info(String.format("Broker %d starting become controller state transition",config.brokerId));
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
            logger.info(String.format("Broker %d is ready to serve as the new controller with epoch %d",config.brokerId, epoch()));
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
    public void onBrokerStartup(List<Integer> newBrokers) throws IOException, InterruptedException {
        logger.info(String.format("New broker startup callback for %s",newBrokers.toString()));

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
        for(Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry : partitionsWithReplicasOnNewBrokers.entrySet()){
            onPartitionReassignment(entry.getKey(), entry.getValue());
        }
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
        logger.info(String.format("Broker failure callback for %s",deadBrokers.toString()));

        List<Integer> deadBrokersThatWereShuttingDown =
                deadBrokers.stream().filter(id -> controllerContext.shuttingDownBrokerIds.remove(id)).collect(Collectors.toList());
        logger.info(String.format("Removed %s from list of shutting down brokers.",deadBrokersThatWereShuttingDown.toString()));

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
        logger.info(String.format("New topic creation callback for %s",newPartitions.toString()));
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
        logger.info(String.format("New partition creation callback for %s",newPartitions.toString()));
        partitionStateMachine.handleStateChanges(newPartitions, new PartitionStateMachine.NewPartition(),partitionStateMachine.noOpPartitionLeaderSelector);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), new ReplicaStateMachine.NewReplica());
        partitionStateMachine.handleStateChanges(newPartitions, new PartitionStateMachine.OnlinePartition(), offlinePartitionSelector);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), new ReplicaStateMachine.OnlineReplica());
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
    public void onPartitionReassignment(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) throws IOException, InterruptedException {
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
            logger.info(String.format("Removed partition %s from the list of reassigned partitions in zookeeper",topicAndPartition.toString()));
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
            // after electing leader, the replicas and isr information changes, so resend the update metadata request
            Set<TopicAndPartition> set = new HashSet<>();
            set.add(topicAndPartition);
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds().stream().collect(Collectors.toList()),set );
        }else{
            logger.info(String.format("New replicas %s for partition %s being ",reassignedReplicas.toString(), topicAndPartition) +
                    "reassigned not yet caught up with the leader");
            // start new replicas
            startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext);
            logger.info(String.format("Waiting for new replicas %s for partition %s being ",reassignedReplicas.toString(), topicAndPartition) +
                    "reassigned to catch up with the leader");
        }
    }

    public void watchIsrChangesForReassignedPartition(String topic,
                                                      int partition,
                                                      ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        ReassignedPartitionsIsrChangeListener isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
                reassignedReplicas.stream().collect(Collectors.toSet()));
        reassignedPartitionContext.isrChangeListener = isrChangeListener;
        // register listener on the leader and isr path to wait until they catch up with the current leader
        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener);
    }

    public void  initiateReassignReplicasForTopicPartition(TopicAndPartition topicAndPartition,
                                                           ReassignedPartitionsContext reassignedPartitionContext) throws IOException {
        List<Integer> newReplicas = reassignedPartitionContext.newReplicas;
        String topic = topicAndPartition.topic();
        int partition = topicAndPartition.partition();
        List<Integer> aliveNewReplicas = newReplicas.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        try {
            List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            if(assignedReplicas != null){
                if(isEquals(assignedReplicas,newReplicas)) {
                    throw new KafkaException(String.format("Partition %s to be reassigned is already assigned to replicas",topicAndPartition.toString()) +
                            String.format(" %s. Ignoring request for partition reassignment",newReplicas.toString()));
                } else {
                    if(isEquals(aliveNewReplicas , newReplicas)) {
                        logger.info(String.format("Handling reassignment of partition %s to new replicas %s",topicAndPartition.toString(), newReplicas.toString()));
                        // first register ISR change listener
                        watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext);
                        controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext);
                        onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                    } else {
                        // some replica in RAR is not alive. Fail partition reassignment
                        throw new KafkaException(String.format("Only %s replicas out of the new set of replicas",aliveNewReplicas.toString()) +
                                String.format(" %s for partition %s to be reassigned are alive. ",newReplicas.toString(), topicAndPartition) +
                                "Failing partition reassignment");
                    }
                }
            }else{
                throw new KafkaException(String
                        .format("Attempt to reassign partition %s that doesn't exist",topicAndPartition.toString()));
            }
        } catch (Throwable e){
            logger.error(String.format("Error completing reassignment of partition %s",topicAndPartition.toString()), e);
            // remove the partition from the admin path to unblock the admin client
            removePartitionFromReassignedPartitions(topicAndPartition);
        }
    }
    private boolean isEquals(List<Integer> list1,List<Integer> list2) {
        if (null != list1 && null != list2) {
            if (list1.containsAll(list2) && list2.containsAll(list1)) {
                return true;
            }
            return false;
        }
        return true;
    }

    public void onPreferredReplicaElection(Set<TopicAndPartition> partitions) {
        logger.info(String.format("Starting preferred replica leader election for partitions %s",partitions.toString()));
        try {
            controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitions);
            partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(), preferredReplicaPartitionLeaderSelector);
        } catch(Throwable e) {
            logger. error(String.format("Error completing preferred replica leader election for partitions %s",partitions.toString()), e);
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
        logger.info(String.format("Controller %d incremented epoch to %d",config.brokerId, controllerContext.epoch));
    }

    private void registerSessionExpirationListener()  {
        zkClient.subscribeStateChanges(new SessionExpirationListener());
    }

    private void initializeControllerContext() throws IOException {
        controllerContext.liveBrokers(ZkUtils.getAllBrokersInCluster(zkClient).stream().collect(Collectors.toSet()));
        controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).stream().collect(Collectors.toSet());
        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.stream().collect(Collectors.toList()));
        controllerContext.partitionLeadershipInfo = new HashMap<>();
        controllerContext.shuttingDownBrokerIds = new HashSet<>();
        // update the leader and isr cache for all existing partitions from Zookeeper
        updateLeaderAndIsrCache();
        // start the channel manager
        startChannelManager();
        logger.info(String.format("Currently active brokers in the cluster: %s",controllerContext.liveBrokerIds().toString()));
        logger.info(String.format("Currently shutting brokers in the cluster: %s",controllerContext.shuttingDownBrokerIds.toString()));
        logger.info(String.format("Current list of topics in the cluster: %s",controllerContext.allTopics.toString()));
    }

    private void initializeAndMaybeTriggerPartitionReassignment() throws IOException {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // check if they are already completed
        List<TopicAndPartition> reassignedPartitions = new ArrayList<>();
        for (Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry : partitionsBeingReassigned.entrySet()) {
            List<Integer> list = controllerContext.partitionReplicaAssignment.get(entry.getKey());
            if(isEquals(list,entry.getValue().newReplicas)){
                reassignedPartitions.add(entry.getKey());
            }
        }
        for(TopicAndPartition p:reassignedPartitions){
            removePartitionFromReassignedPartitions(p);
        }
        Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsToReassign = new HashMap<>();
        partitionsToReassign.putAll(partitionsBeingReassigned);
        reassignedPartitions.forEach(p -> partitionsToReassign.remove(p));

        logger.info(String.format("Partitions being reassigned: %s",partitionsBeingReassigned.toString()));
        logger.info(String.format("Partitions already reassigned: %s",reassignedPartitions.toString()));
        logger.info(String.format("Resuming reassignment of partitions: %s",partitionsToReassign.toString()));

        for(Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry : partitionsToReassign.entrySet()){
            initiateReassignReplicasForTopicPartition(entry.getKey(), entry.getValue());
        }
    }

    private void initializeAndMaybeTriggerPreferredReplicaElection() throws IOException {
        // read the partitions undergoing preferred replica election from zookeeper path
        Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient);
        // check if they are already completed
        Set<TopicAndPartition>  partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.stream().filter(partition ->
                controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader == controllerContext.partitionReplicaAssignment.get(partition).get(0)).collect(Collectors.toSet());
        controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitionsUndergoingPreferredReplicaElection);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsThatCompletedPreferredReplicaElection);
        logger.info(String.format("Partitions undergoing preferred replica election: %s",partitionsUndergoingPreferredReplicaElection.toString()));
        logger.info(String.format("Partitions that completed preferred replica election: %s",partitionsThatCompletedPreferredReplicaElection.toString()));
        logger.info(String.format("Resuming preferred replica election for partitions: %s",controllerContext.partitionsUndergoingPreferredReplicaElection.toString()));
        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.stream().collect(Collectors.toSet()));
    }

    private void startChannelManager() throws IOException {
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
            logger.info(String.format("Leader %s for partition %s being reassigned, ",currentLeader, topicAndPartition.toString()) +
                    String.format("is not in the new list of replicas %s. Re-electing leader",reassignedReplicas.toString()));
            // move the leader to one of the alive and caught up new replicas
            Set<TopicAndPartition> partitions = new HashSet<>();
            partitions.add(topicAndPartition);
            partitionStateMachine.handleStateChanges(partitions, new PartitionStateMachine.OnlinePartition(), reassignedPartitionLeaderSelector);
        } else {
            // check if the leader is alive or not
           boolean isContains = controllerContext.liveBrokerIds().contains(currentLeader);
           if(isContains){
               logger.info(String.format("Leader %s for partition %s being reassigned, ", currentLeader, topicAndPartition.toString()) +
                       String.format("is already in the new list of replicas %s and is alive",reassignedReplicas.toString()));
           }else{
               logger.info(String.format("Leader %s for partition %s being reassigned, ",currentLeader, topicAndPartition.toString()) +
                       String.format("is already in the new list of replicas %s but is dead",reassignedReplicas.toString()));
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
        logger.info(String.format("Updated assigned replicas for partition %s being reassigned to %s ",topicAndPartition.toString(), reassignedReplicas.toString()));
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
        Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned = new HashMap<>();
        for (Map.Entry<TopicAndPartition, KafkaController.ReassignedPartitionsContext> entry : updatedPartitionsBeingReassigned.entrySet()) {
            List<Integer> list = partitionsToBeReassigned.get(entry.getKey());
            if(list == null){
                list = new ArrayList<>();
                partitionsToBeReassigned.put(entry.getKey(),list);
            }
            list.addAll(entry.getValue().newReplicas);
        }
        ZkUtils.updatePartitionReassignmentData(zkClient, partitionsToBeReassigned);
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
            logger.debug(String.format("Updated path %s with %s for replica assignment",zkPath, jsonPartitionMap));
        } catch (ZkNoNodeException e){
           throw new IllegalStateException(String.format("Topic %s doesn't exist",topicAndPartition.topic()));
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
                logger.info(String.format("Partition %s completed preferred replica leader election. New leader is %d",partition, preferredReplica));
            } else {
                logger.warn(String.format("Partition %s failed to complete preferred replica leader election. Leader is %d",partition, currentLeader));
            }
        }
        ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsToBeRemoved);
    }

    private Set<PartitionAndReplica> getAllReplicasForPartition(Set<TopicAndPartition> partitions){
        Set<PartitionAndReplica> res = new HashSet<>();
        for(TopicAndPartition p : partitions){
            List<Integer> replicas = controllerContext.partitionReplicaAssignment.get(p);
            if(replicas == null){
                continue;
            }
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
    private void sendUpdateMetadataRequest(List<Integer> brokers, Set<TopicAndPartition> partitions) throws InterruptedException {
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
        logger.debug(String.format("Removing replica %d from ISR %s for partition %s.",replicaId,
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
                           String.format( "means the current controller with epoch %d went through a soft failure and another ",epoch()) +
                           String.format( "controller was elected with epoch %d. Aborting state change by this controller",controllerEpoch));
                if (leaderAndIsr.isr.contains(replicaId)) {
                    // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
                    int newLeader ;
                    if(replicaId == leaderAndIsr.leader) newLeader = -1; else newLeader = leaderAndIsr.leader;
                    LeaderAndIsrRequest.LeaderAndIsr newLeaderAndIsr = new LeaderAndIsrRequest.LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                    leaderAndIsr.isr.stream().filter(b -> b != replicaId).collect(Collectors.toList()), leaderAndIsr.zkVersion + 1);
                    // update the new leadership decision in zookeeper or retry
                    Pair<Boolean,Integer> pair = ZkUtils.conditionalUpdatePersistentPath(
                            zkClient,
                            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                            ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch()),
                            leaderAndIsr.zkVersion);
                    newLeaderAndIsr.zkVersion = pair.getValue();

                    finalLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch());
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch);
                    if (pair.getKey())
                        logger.info(String.format("New leader and ISR for partition %s is %s",topicAndPartition.toString(), newLeaderAndIsr.toString()));
                    zkWriteCompleteOrUnnecessary = pair.getKey();
                } else {
                    logger.warn(String
                            .format("Cannot remove replica %d from ISR of %s. Leader = %d ; ISR = %s",replicaId, topicAndPartition.toString(), leaderAndIsr.leader, leaderAndIsr.isr));
                    finalLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(leaderAndIsr, epoch());
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch);
                    zkWriteCompleteOrUnnecessary =  true;
                }
            }else{
                logger.warn(String.format("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.",replicaId, topicAndPartition));
                zkWriteCompleteOrUnnecessary = true;
            }
        }
        return finalLeaderIsrAndControllerEpoch;
    }

    public  class SessionExpirationListener implements IZkStateListener {
        public void handleStateChanged(Watcher.Event.KeeperState var1) throws Exception{

        }

        public void handleNewSession() throws Exception{
             synchronized(controllerContext.controllerLock){
                partitionStateMachine.shutdown();
                replicaStateMachine.shutdown();
                if(controllerContext.controllerChannelManager != null) {
                    controllerContext.controllerChannelManager.shutdown();
                    controllerContext.controllerChannelManager = null;
                }
                controllerElector.elect();
            }
        }

        public void handleSessionEstablishmentError(Throwable var1) throws Exception{

        }
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
                    logger.debug(String.format("Reassigned partitions isr change listener fired for path %s with children %s",dataPath, data));
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
                                logger.info(String
                                        .format("%d/%d replicas have caught up with the leader for partition %s being reassigned.",leaderAndIsr.isr.size(), reassignedReplicas.size(), topicAndPartition.toString()) +
                                        "Resuming partition reassignment");
                                controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                            }
                        }else{
                            logger.error(String
                                    .format("Error handling reassignment of partition %s to replicas %s as it was never created",topicAndPartition.toString(), reassignedReplicas.toString()));
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

    /**
     * Starts the preferred replica leader election for the list of partitions specified under
     * /admin/preferred_replica_election -
     */
   public  class PreferredReplicaElectionListener implements IZkDataListener {

        KafkaController controller;

        public PreferredReplicaElectionListener(KafkaController controller) {
            this.controller = controller;
            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
        }

        ZkClient zkClient ;
        ControllerContext controllerContext ;

        /**
         * Invoked when some partitions are reassigned by the admin command
         * @throws Exception On any error.
         */
        public void handleDataChange(String dataPath, Object data) throws Exception {
            logger.debug(String
                    .format("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s",dataPath, data.toString()));
            Set<TopicAndPartition> partitionsForPreferredReplicaElection = ZkUtils.parsePreferredReplicaElectionData(data.toString());

             synchronized(controllerContext.controllerLock) {
                logger.info(String
                        .format("These partitions are already undergoing preferred replica election: %s",controllerContext.partitionsUndergoingPreferredReplicaElection.toString()));
                partitionsForPreferredReplicaElection.removeAll(controllerContext.partitionsUndergoingPreferredReplicaElection);
                controller.onPreferredReplicaElection(partitionsForPreferredReplicaElection);
            }
        }

        /**
         * @throws Exception
         *             On any error.
         */
        public void handleDataDeleted(String var1) throws Exception {
        }

    }

    class ControllerEpochListener implements IZkDataListener  {

       KafkaController controller;

        public ControllerEpochListener(KafkaController controller) {
            this.controller = controller;
            controllerContext = controller.controllerContext;
            readControllerEpochFromZookeeper();
        }

        ControllerContext controllerContext ;
        /**
         * Invoked when a controller updates the epoch value
         * @throws Exception On any error.
         */
        public void handleDataChange(String datapath, Object data) throws Exception {
            logger.debug("Controller epoch listener fired with new epoch " + data.toString());
            synchronized(controllerContext.controllerLock ) {
                // read the epoch path to get the zk version
                readControllerEpochFromZookeeper();
            }
        }

        /**
         * @throws Exception
         *             On any error.
         */
       public void handleDataDeleted(String var1) throws Exception {
       }

        private void readControllerEpochFromZookeeper() {
            // initialize the controller epoch and zk version by reading from zookeeper
            if(ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
                Pair<String, Stat> epochData = ZkUtils.readDataAndStat(controllerContext.zkClient, ZkUtils.ControllerEpochPath);
                controllerContext.epoch = Integer.parseInt(epochData.getKey());
                controllerContext.epochZkVersion = epochData.getValue().getVersion();
                logger.info(String.format("Initialized controller epoch to %d and zk version %d",controllerContext.epoch, controllerContext.epochZkVersion));
            }
        }
    }

    /**
     * Starts the partition reassignment process unless -
     * 1. Partition previously existed
     * 2. New replicas are the same as existing replicas
     * 3. Any replica in the new set of replicas are dead
     * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
     * partitions.
     */
    public class PartitionsReassignedListener implements IZkDataListener {
        KafkaController controller;

        public PartitionsReassignedListener(KafkaController controller) {
            this.controller = controller;
            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
        }

        ZkClient zkClient;
        ControllerContext controllerContext;

        /**
         * Invoked when some partitions are reassigned by the admin command
         *
         * @throws Exception On any error.
         */
        public void handleDataChange(String dataPath, Object data) throws Exception {
            logger.debug(String
                    .format("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s",dataPath, data.toString()));
            Map<TopicAndPartition, List<Integer>> partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString());
            for(Map.Entry<TopicAndPartition, List<Integer>> entry : partitionsReassignmentData.entrySet()){
                if(!controllerContext.partitionsBeingReassigned.containsKey(entry.getKey())){
                    synchronized(controllerContext.controllerLock ) {
                        ReassignedPartitionsContext context = new ReassignedPartitionsContext(entry.getValue(),null);
                        controller.initiateReassignReplicasForTopicPartition(entry.getKey(), context);
                    }
                }
            }
        }
        public void handleDataDeleted(String var1) throws Exception {
        }
    }
}

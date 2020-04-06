package kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.common.BrokerNotAvailableException;
import kafka.common.ControllerMovedException;
import kafka.common.TopicAndPartition;
import kafka.server.KafkaConfig;
import kafka.server.ZookeeperLeaderElector;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds());
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
        sendUpdateMetadataRequest(newBrokers);
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
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
            case true =>
                // mark the new replicas as online
                reassignedReplicas.foreach { replica =>
                replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
                        replica)), OnlineReplica)
            }
            // check if current leader is in the new replicas list. If not, controller needs to trigger leader election
            moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
            // stop older replicas
            stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext)
            // write the new list of replicas for this partition in zookeeper
            updateAssignedReplicasForPartition(topicAndPartition, reassignedPartitionContext)
            // update the /admin/reassign_partitions path to remove this partition
            removePartitionFromReassignedPartitions(topicAndPartition)
            info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
            // after electing leader, the replicas and isr information changes, so resend the update metadata request
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
            case false =>
                info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
                        "reassigned not yet caught up with the leader")
                // start new replicas
                startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext)
                info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
                        "reassigned to catch up with the leader")
        }
    }

    private def watchIsrChangesForReassignedPartition(topic: String,
                                                      partition: Int,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
                reassignedReplicas.toSet)
        reassignedPartitionContext.isrChangeListener = isrChangeListener
        // register listener on the leader and isr path to wait until they catch up with the current leader
        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
    }

    def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                                  reassignedPartitionContext: ReassignedPartitionsContext) {
        val newReplicas = reassignedPartitionContext.newReplicas
        val topic = topicAndPartition.topic
        val partition = topicAndPartition.partition
        val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        try {
            val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
            assignedReplicasOpt match {
                case Some(assignedReplicas) =>
                    if(assignedReplicas == newReplicas) {
                        throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
                                " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
                    } else {
                        if(aliveNewReplicas == newReplicas) {
                            info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
                            // first register ISR change listener
                            watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
                            controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
                            onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                        } else {
                            // some replica in RAR is not alive. Fail partition reassignment
                            throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.mkString(",")) +
                                    " %s for partition %s to be reassigned are alive. ".format(newReplicas.mkString(","), topicAndPartition) +
                                    "Failing partition reassignment")
                        }
                    }
                case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
                        .format(topicAndPartition))
            }
        } catch {
            case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
                // remove the partition from the admin path to unblock the admin client
                removePartitionFromReassignedPartitions(topicAndPartition)
        }
    }

    def onPreferredReplicaElection(partitions: Set[TopicAndPartition]) {
        info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
        try {
            controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions
            partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
        } catch {
            case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
        } finally {
            removePartitionsFromPreferredReplicaElection(partitions)
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
     * is the controller. It merely registers the session expiration listener and starts the controller leader
     * elector
     */
    def startup() = {
        controllerContext.controllerLock synchronized {
            info("Controller starting up");
            registerSessionExpirationListener()
            isRunning = true
            controllerElector.startup
            info("Controller startup complete")
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
     */
    def shutdown() = {
        controllerContext.controllerLock synchronized {
            isRunning = false
            partitionStateMachine.shutdown()
            replicaStateMachine.shutdown()
            if(controllerContext.controllerChannelManager != null) {
                controllerContext.controllerChannelManager.shutdown()
                controllerContext.controllerChannelManager = null
                info("Controller shutdown complete")
            }
        }
    }

    def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)
    }

    def incrementControllerEpoch(zkClient: ZkClient) = {
        try {
            var newControllerEpoch = controllerContext.epoch + 1
            val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
                    ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
            if(!updateSucceeded)
                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
            else {
                controllerContext.epochZkVersion = newVersion
                controllerContext.epoch = newControllerEpoch
            }
        } catch {
            case nne: ZkNoNodeException =>
                // if path doesn't exist, this is the first controller whose epoch should be 1
                // the following call can still fail if another controller gets elected between checking if the path exists and
                // trying to create the controller epoch path
                try {
                    zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
                    controllerContext.epoch = KafkaController.InitialControllerEpoch
                    controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
                } catch {
                case e: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
                        "Aborting controller startup procedure")
                case oe: Throwable => error("Error while incrementing controller epoch", oe)
            }
            case oe: Throwable => error("Error while incrementing controller epoch", oe)

        }
        info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
    }

    private def registerSessionExpirationListener() = {
        zkClient.subscribeStateChanges(new SessionExpirationListener())
    }

    private def initializeControllerContext() {
        controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
        controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet
        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.toSeq)
        controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
        controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
        // update the leader and isr cache for all existing partitions from Zookeeper
        updateLeaderAndIsrCache()
        // start the channel manager
        startChannelManager()
        info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
        info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
        info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
    }

    private def initializeAndMaybeTriggerPartitionReassignment() {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
        // check if they are already completed
        val reassignedPartitions = partitionsBeingReassigned.filter(partition =>
                controllerContext.partitionReplicaAssignment(partition._1) == partition._2.newReplicas).map(_._1)
        reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
        var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
        partitionsToReassign ++= partitionsBeingReassigned
        partitionsToReassign --= reassignedPartitions

        info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
        info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
        info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))

        partitionsToReassign.foreach { topicPartitionToReassign =>
            initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
        }
    }

    private def initializeAndMaybeTriggerPreferredReplicaElection() {
        // read the partitions undergoing preferred replica election from zookeeper path
        val partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient)
        // check if they are already completed
        val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter(partition =>
                controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == controllerContext.partitionReplicaAssignment(partition).head)
        controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
        info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
        info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
        info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
    }

    private def startChannelManager() {
        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config)
        controllerContext.controllerChannelManager.startup()
    }

    private def updateLeaderAndIsrCache() {
        val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet)
        for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
        controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
    }

    private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
        getLeaderAndIsrForPartition(zkClient, topic, partition) match {
            case Some(leaderAndIsr) =>
                val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
                replicasNotInIsr.isEmpty
            case None => false
        }
    }

    private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                        reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
        if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
            info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                    "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
            // move the leader to one of the alive and caught up new replicas
            partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
        } else {
            // check if the leader is alive or not
            controllerContext.liveBrokerIds.contains(currentLeader) match {
                case true =>
                    info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                            "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
                case false =>
                    info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                            "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
                    partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
            }
        }
    }

    private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val topic = topicAndPartition.topic
        val partition = topicAndPartition.partition
        // send stop replica state change request to the old replicas
        val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
        // first move the replica to offline state (the controller removes it from the ISR)
        oldReplicas.foreach { replica =>
            replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), OfflineReplica)
        }
        // send stop replica command to the old replicas
        oldReplicas.foreach { replica =>
            replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), NonExistentReplica)
        }
    }

    private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
        partitionsAndReplicasForThisTopic.put(topicAndPartition, reassignedReplicas)
        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
        info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, reassignedReplicas.mkString(",")))
        // update the assigned replica list after a successful zookeeper write
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
        // stop watching the ISR changes for this partition
        zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    }

    private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                       reassignedPartitionContext: ReassignedPartitionsContext) {
        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
        // replicas list
        val assignedReplicaSet = Set.empty[Int] ++ controllerContext.partitionReplicaAssignment(topicAndPartition)
        val reassignedReplicaSet = Set.empty[Int] ++ reassignedPartitionContext.newReplicas
        val newReplicas: Seq[Int] = (reassignedReplicaSet -- assignedReplicaSet).toSeq
        newReplicas.foreach { replica =>
            replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
        }
    }

    private def registerReassignedPartitionsListener() = {
        zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, new PartitionsReassignedListener(this))
    }

    private def registerPreferredReplicaElectionListener() {
        zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, new PreferredReplicaElectionListener(this))
    }

    private def registerControllerChangedListener() {
        zkClient.subscribeDataChanges(ZkUtils.ControllerEpochPath, new ControllerEpochListener(this))
    }

    def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
        // read the current list of reassigned partitions from zookeeper
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
        // remove this partition from that list
        val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
        // write the new list to zookeeper
        ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
        // update the cache
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
    }

    def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                           newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
        try {
            val zkPath = ZkUtils.getTopicPath(topicAndPartition.topic)
            val jsonPartitionMap = ZkUtils.replicaAssignmentZkdata(newReplicaAssignmentForTopic.map(e => (e._1.partition.toString -> e._2)))
            ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap)
            debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
        } catch {
            case e: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
            case e2: Throwable => throw new KafkaException(e2.toString)
        }
    }

    def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition]) {
        for(partition <- partitionsToBeRemoved) {
            // check the status
            val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
            val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
            if(currentLeader == preferredReplica) {
                info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
            } else {
                warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
            }
        }
        ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath)
        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
    }

    private def getAllReplicasForPartition(partitions: Set[TopicAndPartition]): Set[PartitionAndReplica] = {
        partitions.map { p =>
            val replicas = controllerContext.partitionReplicaAssignment(p)
            replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
        }.flatten
    }

    /**
     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
     * metadata requests
     * @param brokers The brokers that the update metadata request should be sent to
     * @param partitions The partitions for which the metadata is to be sent
     */
    private def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
        brokerRequestBatch.newBatch()
        brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
        brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)
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
    def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
        val topicAndPartition = TopicAndPartition(topic, partition)
        debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
                controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
        var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
        var zkWriteCompleteOrUnnecessary = false
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            val leaderIsrAndEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
            zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
                case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
                    val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
                    val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
                    if(controllerEpoch > epoch)
                        throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                                "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
                                "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
                    if (leaderAndIsr.isr.contains(replicaId)) {
                        // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
                        val newLeader = if(replicaId == leaderAndIsr.leader) -1 else leaderAndIsr.leader
                        val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                        leaderAndIsr.isr.filter(b => b != replicaId), leaderAndIsr.zkVersion + 1)
                        // update the new leadership decision in zookeeper or retry
                        val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(
                                zkClient,
                                ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                                ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch),
                                leaderAndIsr.zkVersion)
                        newLeaderAndIsr.zkVersion = newVersion

                        finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
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
                case None =>
                    warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
                    true
            }
        }
        finalLeaderIsrAndControllerEpoch
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

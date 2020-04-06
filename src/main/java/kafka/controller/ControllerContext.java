package kafka.controller;

import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import org.I0Itec.zkclient.ZkClient;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ControllerContext {

    public ZkClient zkClient;
    public int zkSessionTimeout;
    public  ControllerChannelManager controllerChannelManager = null;
    public Object controllerLock  = new Object();
    public Set<Integer> shuttingDownBrokerIds = new HashSet<>();
    public Object brokerShutdownLock  = new Object();
    public int epoch = KafkaController.InitialControllerEpoch - 1;
    public int epochZkVersion = KafkaController.InitialControllerEpochZkVersion - 1;
    public  AtomicInteger correlationId = new AtomicInteger(0);
    public Set<String> allTopics = new HashSet<>();
    public Map<TopicAndPartition, List<Integer>> partitionReplicaAssignment = new HashMap<>();
    public Map<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionLeadershipInfo = new HashMap<>();
    public Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsBeingReassigned = new HashMap<>();
    public Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = new HashSet<>();

    public ControllerContext(ZkClient zkClient, int zkSessionTimeout, ControllerChannelManager controllerChannelManager, Object controllerLock, Set<Integer> shuttingDownBrokerIds, Object brokerShutdownLock, int epoch, int epochZkVersion, AtomicInteger correlationId, Set<String> allTopics, Map<TopicAndPartition, List<Integer>> partitionReplicaAssignment, Map<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionLeadershipInfo, Map<TopicAndPartition, KafkaController.ReassignedPartitionsContext> partitionsBeingReassigned, Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection) {
        this.zkClient = zkClient;
        this.zkSessionTimeout = zkSessionTimeout;
        this.controllerChannelManager = controllerChannelManager;
        this.controllerLock = controllerLock;
        this.shuttingDownBrokerIds = shuttingDownBrokerIds;
        this.brokerShutdownLock = brokerShutdownLock;
        this.epoch = epoch;
        this.epochZkVersion = epochZkVersion;
        this.correlationId = correlationId;
        this.allTopics = allTopics;
        this.partitionReplicaAssignment = partitionReplicaAssignment;
        this.partitionLeadershipInfo = partitionLeadershipInfo;
        this.partitionsBeingReassigned = partitionsBeingReassigned;
        this.partitionsUndergoingPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection;
    }

    public ControllerContext(ZkClient zkClient, int zkSessionTimeout) {
        this.zkClient = zkClient;
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public Set<Broker> liveBrokersUnderlying = new HashSet<>();
    public Set<Integer> liveBrokerIdsUnderlying = new HashSet<>();

    // setter
    public void liveBrokers(Set<Broker> brokers) {
        liveBrokersUnderlying = brokers;
        liveBrokerIdsUnderlying = liveBrokersUnderlying.stream().map(Broker::id).collect(Collectors.toSet());
    }

    // getter
    public Set<Broker> liveBrokers() {
        return liveBrokersUnderlying.stream().filter(broker -> !shuttingDownBrokerIds.contains(broker.id())).collect(Collectors.toSet());
    }
    public Set<Integer> liveBrokerIds(){
        return liveBrokerIdsUnderlying.stream().filter(brokerId -> !shuttingDownBrokerIds.contains(brokerId)).collect(Collectors.toSet());
    }

    public Set<Broker> liveOrShuttingDownBrokers() {
        return liveBrokersUnderlying;
    }

    public Set<Integer> liveOrShuttingDownBrokerIds() {
        return liveBrokerIdsUnderlying;
    }
}

package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;

/**
 * This API selects a new leader for the input partition -
 * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
 * 2. Else, it picks some alive broker from the assigned replica list as the new leader
 * 3. If no broker in the assigned replica list is alive, it throws NoReplicaOnlineException
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
public class OfflinePartitionLeaderSelector implements PartitionLeaderSelector {

    public ControllerContext controllerContext;

    public OfflinePartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){

    }
}

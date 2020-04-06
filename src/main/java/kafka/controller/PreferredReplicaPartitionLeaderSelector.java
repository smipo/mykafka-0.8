package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;
/**
 * Picks the preferred replica as the new leader if -
 * 1. It is already not the current leader
 * 2. It is alive
 */
public class PreferredReplicaPartitionLeaderSelector implements PartitionLeaderSelector  {

    public ControllerContext controllerContext;

    public PreferredReplicaPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){

    }
}

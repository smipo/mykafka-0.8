package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;
/**
 * Picks one of the alive in-sync reassigned replicas as the new leader.
 */
public class ReassignedPartitionLeaderSelector implements PartitionLeaderSelector {

    public ControllerContext controllerContext;

    public ReassignedPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }
    /**
     * The reassigned replicas are already in the ISR when selectLeader is called.
     */
    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){

    }
}

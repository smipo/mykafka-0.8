package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;

/**
 * Picks one of the alive replicas (other than the current leader) in ISR as
 * new leader, fails if there are no other replicas in ISR.
 */
public class ControlledShutdownLeaderSelector implements PartitionLeaderSelector   {

    public ControllerContext controllerContext;

    public ControlledShutdownLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){

    }
}

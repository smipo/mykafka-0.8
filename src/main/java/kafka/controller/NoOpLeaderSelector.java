package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;

public class NoOpLeaderSelector implements PartitionLeaderSelector {

    public ControllerContext controllerContext;

    public NoOpLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){

    }
}

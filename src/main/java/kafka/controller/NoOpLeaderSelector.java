package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.util.List;

public class NoOpLeaderSelector implements PartitionLeaderSelector {

    private static Logger logger = Logger.getLogger(NoOpLeaderSelector.class);

    public ControllerContext controllerContext;

    public NoOpLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){
        logger.warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.");
        return new Pair<>(currentLeaderAndIsr, controllerContext.partitionReplicaAssignment.get(topicAndPartition));
    }
}

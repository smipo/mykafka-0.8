package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.util.List;
/**
 * Picks the preferred replica as the new leader if -
 * 1. It is already not the current leader
 * 2. It is alive
 */
public class PreferredReplicaPartitionLeaderSelector implements PartitionLeaderSelector  {

    private static Logger logger = Logger.getLogger(PreferredReplicaPartitionLeaderSelector.class);

    public ControllerContext controllerContext;

    public PreferredReplicaPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){
        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        int preferredReplica = assignedReplicas.get(0);
        // check if preferred replica is the current leader
        int currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        if (currentLeader == preferredReplica) {
            throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                    .format(preferredReplica+"", topicAndPartition));
        } else {
            logger.info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader+"", topicAndPartition) +
                    " Trigerring preferred replica leader election");
            // check if preferred replica is not the current leader and is alive and in the isr
            if (controllerContext.liveBrokerIds().contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
                return new Pair<>(new LeaderAndIsrRequest.LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
                        currentLeaderAndIsr.zkVersion + 1), assignedReplicas);
            } else {
                throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica+"") +
                        "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition.toString(), currentLeaderAndIsr));
            }
        }
    }
}

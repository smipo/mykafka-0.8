package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Picks one of the alive replicas (other than the current leader) in ISR as
 * new leader, fails if there are no other replicas in ISR.
 */
public class ControlledShutdownLeaderSelector implements PartitionLeaderSelector   {

    private static Logger logger = Logger.getLogger(ControlledShutdownLeaderSelector.class);

    public ControllerContext controllerContext;

    public ControlledShutdownLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){
        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;

        int currentLeader = currentLeaderAndIsr.leader;

        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        Set<Integer> liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds();
        List<Integer> liveAssignedReplicas = assignedReplicas.stream().filter(r -> liveOrShuttingDownBrokerIds.contains(r)).collect(Collectors.toList());

        List<Integer> newIsr = currentLeaderAndIsr.isr.stream().filter(brokerId -> brokerId != currentLeader &&
                !controllerContext.shuttingDownBrokerIds.contains(brokerId)).collect(Collectors.toList());
        if(newIsr == null || newIsr.isEmpty()){
            throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides current leader %d and" +
                    " shutting down brokers %s").format(currentLeaderAndIsr.isr.toString(), topicAndPartition, currentLeader, controllerContext.shuttingDownBrokerIds.toString()));

        }else{
            logger.debug("Partition %s : current leader = %d, new leader = %d"
                    .format(topicAndPartition.toString(), currentLeader, newIsr.get(0)));
           return new Pair<>(new LeaderAndIsrRequest.LeaderAndIsr(newIsr.get(0), currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1),
                    liveAssignedReplicas);
        }
    }
}

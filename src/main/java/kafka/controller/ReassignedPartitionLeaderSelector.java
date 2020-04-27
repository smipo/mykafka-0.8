package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.util.List;
import java.util.stream.Collectors;

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
        List<Integer> reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned.get(topicAndPartition).newReplicas;
        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
        List<Integer>  aliveReassignedInSyncReplicas = reassignedInSyncReplicas.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        if(aliveReassignedInSyncReplicas == null || aliveReassignedInSyncReplicas.isEmpty()){
            if(reassignedInSyncReplicas.size() == 0){
                throw new StateChangeFailedException(String.format("List of reassigned replicas for partition " +
                        " %s is empty. Current leader and ISR: [%s]",topicAndPartition.toString(), currentLeaderAndIsr));
            }else{
                throw new StateChangeFailedException(String.format("None of the reassigned replicas for partition " +
                        "%s are alive. Current leader and ISR: [%s]",topicAndPartition.toString(), currentLeaderAndIsr));
            }
        }else{
           return  new Pair<>(new LeaderAndIsrRequest.LeaderAndIsr(aliveReassignedInSyncReplicas.get(0), currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
                    currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas);
        }
    }
}

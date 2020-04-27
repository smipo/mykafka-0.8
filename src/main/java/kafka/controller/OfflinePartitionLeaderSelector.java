package kafka.controller;

import kafka.api.LeaderAndIsrRequest;
import kafka.common.NoReplicaOnlineException;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This API selects a new leader for the input partition -
 * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
 * 2. Else, it picks some alive broker from the assigned replica list as the new leader
 * 3. If no broker in the assigned replica list is alive, it throws NoReplicaOnlineException
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
public class OfflinePartitionLeaderSelector implements PartitionLeaderSelector {

    private static Logger logger = Logger.getLogger(OfflinePartitionLeaderSelector.class);

    public ControllerContext controllerContext;

    public OfflinePartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public Pair<LeaderAndIsrRequest.LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsrRequest.LeaderAndIsr currentLeaderAndIsr){
        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        if(assignedReplicas == null){
            throw new NoReplicaOnlineException(String.format("Partition %s doesn't have",topicAndPartition.toString()) + "replicas assigned to it");
        }
        List<Integer> liveAssignedReplicasToThisPartition = assignedReplicas.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        List<Integer> liveBrokersInIsr = currentLeaderAndIsr.isr.stream().filter(r -> controllerContext.liveBrokerIds().contains(r)).collect(Collectors.toList());
        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
        LeaderAndIsrRequest.LeaderAndIsr newLeaderAndIsr ;
        if(liveBrokersInIsr.isEmpty()){
            logger.debug(String
                    .format("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s",topicAndPartition.toString(), liveAssignedReplicasToThisPartition.toString()));
            if(liveAssignedReplicasToThisPartition.isEmpty()){
                throw new NoReplicaOnlineException((String.format("No replica for partition " +
                        "%s is alive. Live brokers are: [%s],",topicAndPartition.toString(), controllerContext.liveBrokerIds().toString())) +
                        String.format(" Assigned replicas are: [%s]",assignedReplicas.toString()));
            }else{
                int newLeader = liveAssignedReplicasToThisPartition.get(0);
                logger.warn(String
                        .format("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss.",topicAndPartition.toString(), newLeader, liveAssignedReplicasToThisPartition.toString()));
                List<Integer> isr = new ArrayList<>();
                isr.add(newLeader);
                newLeaderAndIsr = new LeaderAndIsrRequest.LeaderAndIsr(newLeader, currentLeaderEpoch + 1, isr, currentLeaderIsrZkPathVersion + 1);
            }
        }else{
            int newLeader = liveBrokersInIsr.get(0);
            logger.debug(String
                    .format("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader.",topicAndPartition.toString(), newLeader, liveBrokersInIsr.toString()));
            newLeaderAndIsr = new LeaderAndIsrRequest.LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr, currentLeaderIsrZkPathVersion + 1);
        }
        logger.info(String.format("Selected new leader and ISR %s for offline partition %s",newLeaderAndIsr.toString(), topicAndPartition.toString()));
        return new Pair<>(newLeaderAndIsr, liveAssignedReplicasToThisPartition);
    }
}

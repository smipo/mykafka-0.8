package kafka.admin;

import kafka.common.AdministrationException;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class AdminUtils {

    private static Logger logger = Logger.getLogger(AdminUtils.class);

    public static Random rand = new Random();
    public static int AdminEpoch = -1;

    /**
     * There are 2 goals of replica assignment:
     * 1. Spread the replicas evenly among brokers.
     * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
     *
     * To achieve this goal, we:
     * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
     * 2. Assign the remaining replicas of each partition with an increasing shift.
     *
     * Here is an example of assigning
     * broker-0  broker-1  broker-2  broker-3  broker-4
     * p0        p1        p2        p3        p4       (1st replica)
     * p5        p6        p7        p8        p9       (1st replica)
     * p4        p0        p1        p2        p3       (2nd replica)
     * p8        p9        p5        p6        p7       (2nd replica)
     * p3        p4        p0        p1        p2       (3nd replica)
     * p7        p8        p9        p5        p6       (3nd replica)
     */
    public static Map<Integer, List<Integer>> assignReplicasToBrokers(List<Integer> brokerList, int nPartitions,int replicationFactor,
                                                                      int fixedStartIndex, int startPartitionId) {
        if (nPartitions <= 0)
            throw new AdministrationException("number of partitions must be larger than 0");
        if (replicationFactor <= 0)
            throw new AdministrationException("replication factor must be larger than 0");
        if (replicationFactor > brokerList.size())
            throw new AdministrationException("replication factor: " + replicationFactor +
                    " larger than available brokers: " + brokerList.size());
        Map<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = fixedStartIndex >= 0?fixedStartIndex : rand.nextInt(brokerList.size());
        int currentPartitionId = startPartitionId >= 0?startPartitionId : 0;

        int nextReplicaShift = fixedStartIndex >= 0? fixedStartIndex : rand.nextInt(brokerList.size());
        for (int i = 0 ; i < nPartitions;i++) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0))
                nextReplicaShift += 1;
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List<Integer> replicaList = new ArrayList<>();
            replicaList.add(brokerList.get(firstReplicaIndex));
            for (int j = 0 ;j < replicationFactor - 1;j++)
                replicaList .add(brokerList.get(getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size())));
            ret.put(currentPartitionId, replicaList.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
            currentPartitionId = currentPartitionId + 1;
        }
        return ret;
    }

    public static void createOrUpdateTopicPartitionAssignmentPathInZK(String topic, Map<Integer, List<Integer>> replicaAssignment, ZkClient zkClient, boolean update) {
        try {
            String zkPath = ZkUtils.getTopicPath(topic);
            Map<String,List<Integer>> map = new HashMap<>();
            for(Map.Entry<Integer, List<Integer>> entry : replicaAssignment.entrySet()){
                map.put(String.valueOf(entry.getKey()),entry.getValue());
            }
            String jsonPartitionData = ZkUtils.replicaAssignmentZkdata(map);
            if (!update) {
                logger.info("Topic creation " + jsonPartitionData);
                ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData);
            } else {
                logger.info("Topic update " + jsonPartitionData);
                ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData);
            }
            logger.debug(String.format("Updated path %s with %s for replica assignment",zkPath, jsonPartitionData));
        } catch(ZkNodeExistsException e) {
             throw new TopicExistsException(String.format("topic %s already exists",topic));
        }catch (Throwable e2){
            throw new AdministrationException(e2.toString());
        }
    }

    private static int getWrappedIndex(int firstReplicaIndex,int secondReplicaShift,int replicaIndex, int nBrokers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (firstReplicaIndex + shift) % nBrokers;
    }

}

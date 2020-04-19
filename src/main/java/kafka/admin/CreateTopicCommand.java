package kafka.admin;

import kafka.common.AdministrationException;
import kafka.common.Topic;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateTopicCommand {

    private static Logger logger = Logger.getLogger(CreateTopicCommand.class);

    public static void createTopic(ZkClient zkClient, String topic, int numPartitions, int replicationFactor, String replicaAssignmentStr) {
        Topic.validate(topic);

        List<Integer> brokerList = ZkUtils.getSortedBrokerList(zkClient);

        Map<Integer, List<Integer>> partitionReplicaAssignment = replicaAssignmentStr == ""?
                AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor,-1,-1)
                :
                getManualReplicaAssignment(replicaAssignmentStr, brokerList);
        logger.debug("Replica assignment list for %s is %s".format(topic, partitionReplicaAssignment));
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionReplicaAssignment, zkClient,false);
    }

    public static Map<Integer, List<Integer>> getManualReplicaAssignment(String replicaAssignmentList,List<Integer> availableBrokerList) {
        String[] partitionList = replicaAssignmentList.split(",");
        Map<Integer, List<Integer>> ret = new HashMap<>();
        for (int i = 0 ;i < partitionList.length;i++) {
            List<Integer> brokerList = new ArrayList<>();
            for(String s:partitionList[i].split(":")){
                brokerList.add(Integer.parseInt(s.trim()));
            }
            if (brokerList.size() <= 0)
                throw new AdministrationException("replication factor must be larger than 0");
            if (brokerList.size() != brokerList.stream().collect(Collectors.toSet()).size())
                throw new AdministrationException("duplicate brokers in replica assignment: " + brokerList);
            if (!brokerList.stream().collect(Collectors.toSet()).containsAll(availableBrokerList))
                throw new AdministrationException("some specified brokers not available. specified brokers: " + brokerList.toString() +
                        "available broker:" + availableBrokerList.toString());
            ret.put(i, brokerList);
            if (ret.get(i).size() != ret.get(0).size())
                throw new AdministrationException("partition " + i + " has different replication factor: " + brokerList);
        }
        return ret;
    }
}

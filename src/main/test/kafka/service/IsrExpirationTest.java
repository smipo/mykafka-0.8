package kafka.service;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.log.Log;
import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;
import kafka.utils.Pair;
import kafka.utils.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class IsrExpirationTest {

    Map<Pair<String, Integer>, List<Integer>> topicPartitionIsr = new HashMap<>();
    List<KafkaConfig> configs = new ArrayList<>();
    String topic = "foo";

    @Before
    public void init() throws IOException {
        List<Properties> properties = TestUtils.createBrokerConfigs(2);
        for(Properties propertie:properties){
            configs.add(new KafkaConfig(propertie){
                long replicaLagTimeMaxMs = 100L;
                long replicaLagMaxMessages = 10L;
            });
        }
    }

    @Test
    public void testIsrExpirationForStuckFollowers() throws InterruptedException {
        long time = System.currentTimeMillis();
        Log log = getLogWithLogEndOffset(15L, 2) ;// set logEndOffset for leader to 15L

        // create one partition and all replicas
        Partition partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.get(0), log);
        assertEquals("All replicas should be in ISR", configs.stream().map(c->c.brokerId).collect(Collectors.toSet()), partition0.inSyncReplicas.stream().map(x->x.brokerId).collect(Collectors.toSet()));
        Replica leaderReplica = partition0.getReplica(configs.get(0).brokerId);

                // let the follower catch up to 10
        partition0.assignedReplicas().remove(leaderReplica);
        partition0.assignedReplicas().forEach(r -> r.logEndOffset(10L));
        Set<Replica> partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.get(0).replicaLagTimeMaxMs, configs.get(0).replicaLagMaxMessages);
        assertEquals("No replica should be out of sync", Sets.newHashSet(), partition0OSR.stream().map(x->x.brokerId).collect(Collectors.toSet()));

        // let some time pass
        Thread.sleep(150);

        // now follower (broker id 1) has caught up to only 10, while the leader is at 15 AND the follower hasn't
        // pulled any data for > replicaMaxLagTimeMs ms. So it is stuck
        partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.get(0).replicaLagTimeMaxMs, configs.get(0).replicaLagMaxMessages);
        assertEquals("Replica 1 should be out of sync", Sets.newHashSet(configs.get(configs.size() - 1).brokerId), partition0OSR.stream().map(x->x.brokerId).collect(Collectors.toSet()));
        EasyMock.verify(log);
    }

    @Test
    public void testIsrExpirationForSlowFollowers() {
        long time = System.currentTimeMillis();
        // create leader replica
        Log log = getLogWithLogEndOffset(15L, 1);
        // add one partition
        Partition partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.get(0), log);
        assertEquals("All replicas should be in ISR", configs.stream().map(c->c.brokerId).collect(Collectors.toSet()), partition0.inSyncReplicas.stream().map(x->x.brokerId));
        Replica leaderReplica = partition0.getReplica(configs.get(0).brokerId);
                // set remote replicas leo to something low, like 4
        partition0.assignedReplicas().remove(leaderReplica);
        partition0.assignedReplicas().forEach(r -> r.logEndOffset(4L));

        // now follower (broker id 1) has caught up to only 4, while the leader is at 15. Since the gap it larger than
        // replicaMaxLagBytes, the follower is out of sync.
        Set<Replica> partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.get(0).replicaLagTimeMaxMs, configs.get(0).replicaLagMaxMessages);
        assertEquals("Replica 1 should be out of sync",  Sets.newHashSet(configs.get(configs.size() - 1).brokerId), partition0OSR.stream().map(x->x.brokerId).collect(Collectors.toSet()));

        EasyMock.verify(log);
    }

    private Partition getPartitionWithAllReplicasInIsr(String topic,int partitionId,long time,KafkaConfig config, Log localLog) {
        int leaderId=config.brokerId;
        ReplicaManager replicaManager = new ReplicaManager(config, time, null, null, null, new AtomicBoolean(false));
        Partition partition = replicaManager.getOrCreatePartition(topic, partitionId, 1);
        Replica leaderReplica = new Replica(leaderId, partition, time, 0,localLog);
        List<Replica> allReplicas = getFollowerReplicas(partition, leaderId, time);
        allReplicas.add(leaderReplica);
        allReplicas.forEach(r -> partition.addReplicaIfNotExists(r));
        // set in sync replicas for this partition to all the assigned replicas
        partition.inSyncReplicas = allReplicas.stream().collect(Collectors.toSet());
        // set the leader and its hw and the hw update time
        partition.leaderReplicaIdOpt = leaderId;
        return partition;
    }

    private Log getLogWithLogEndOffset(long logEndOffset, int expectedCalls) {
        Log log1 = EasyMock.createMock(Log.class);
        EasyMock.expect(log1.logEndOffset()).andReturn(logEndOffset).times(expectedCalls);
        EasyMock.replay(log1);
        return log1;
    }

    private List<Replica> getFollowerReplicas(Partition partition, int leaderId, long time) {
        List<Replica> list = configs.stream().filter(c->c.brokerId != leaderId).map(config->new Replica(config.brokerId, partition, time,0,null)).collect(Collectors.toList());
        return list;
    }
}

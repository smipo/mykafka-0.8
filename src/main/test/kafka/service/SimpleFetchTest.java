package kafka.service;

import com.google.common.collect.Maps;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.api.OffsetResponse;
import kafka.api.RequestOrResponse;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.TopicAndPartition;
import kafka.controller.KafkaController;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.server.KafkaApis;
import kafka.server.KafkaConfig;
import kafka.server.ReplicaFetcherManager;
import kafka.server.ReplicaManager;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SimpleFetchTest {

    List<KafkaConfig> configs = new ArrayList<>();
    String topic = "foo";
    int partitionId = 0;

    @Before
    public void init() throws Exception{
        configs = TestUtils.createBrokerConfigs(2).stream().map(p -> new KafkaConfig(p)).collect(Collectors.toList());
    }
    /**
     * The scenario for this test is that there is one topic, "test-topic", on broker "0" that has
     * one  partition with one follower replica on broker "1".  The leader replica on "0"
     * has HW of "5" and LEO of "20".  The follower on broker "1" has a local replica
     * with a HW matching the leader's ("5") and LEO of "15", meaning it's not in-sync
     * but is still in ISR (hasn't yet expired from ISR).
     *
     * When a normal consumer fetches data, it only should only see data upto the HW of the leader,
     * in this case up an offset of "5".
     */
    @Test
    public void testNonReplicaSeesHwWhenFetching() throws Exception {
        /* setup */
        long time = System.currentTimeMillis();
        long leo = 20;
        long hw = 5;
        int fetchSize = 100;
        Message messages = new Message("test-message".getBytes());

        // create nice mock since we don't particularly care about zkclient calls
        ZkClient zkClient = EasyMock.createNiceMock(ZkClient.class);
        EasyMock.expect(zkClient.exists(ZkUtils.ControllerEpochPath)).andReturn(false);
        EasyMock.replay(zkClient);

        Log log = EasyMock.createMock(Log.class);
        EasyMock.expect(log.logEndOffset()).andReturn(leo).anyTimes();
        EasyMock.expect(log.read(0, fetchSize, hw)).andReturn(new ByteBufferMessageSet(messages));
        EasyMock.replay(log);

        LogManager logManager = EasyMock.createMock(LogManager.class);
        EasyMock.expect(logManager.getLog(topic, partitionId)).andReturn(log).anyTimes();
        EasyMock.expect(logManager.config).andReturn(configs.get(0)).anyTimes();
        EasyMock.replay(logManager);

        ReplicaManager replicaManager = EasyMock.createMock(ReplicaManager.class);
        EasyMock.expect(replicaManager.config).andReturn(configs.get(0));
        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
        EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(ReplicaFetcherManager.class));
        EasyMock.expect(replicaManager.zkClient).andReturn(zkClient);
        EasyMock.replay(replicaManager);

        Partition partition = getPartitionWithAllReplicasInISR(topic, partitionId, time, configs.get(0).brokerId, log, hw, replicaManager);
        partition.getReplica(configs.get(1).brokerId).logEndOffset(leo - 5L) ;

        EasyMock.reset(replicaManager);
        EasyMock.expect(replicaManager.config).andReturn(configs.get(0)).anyTimes();
        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal()).anyTimes();
        EasyMock.replay(replicaManager);

        KafkaController controller = EasyMock.createMock(KafkaController.class);

        // start a request channel with 2 processors and a queue size of 5 (this is more or less arbitrary)
        // don't provide replica or leader callbacks since they will not be tested here
        RequestChannel requestChannel = new RequestChannel(2, 5);
        KafkaApis apis = new KafkaApis(requestChannel, replicaManager, zkClient, configs.get(0).brokerId, controller);

        // This request (from a follower) wants to read up to 2*HW but should only get back up to HW bytes into the log
        FetchRequest goodFetch = new FetchRequest.FetchRequestBuilder()
                .replicaId(RequestOrResponse.OrdinaryConsumerId)
                .addFetch(topic, partitionId, 0, fetchSize)
                .build();
        ByteBuffer goodFetchBB = TestUtils.createRequestByteBuffer(goodFetch);

        // send the request
        apis.handleFetchRequest(new RequestChannel.Request(1, 5, goodFetchBB, 1,new InetSocketAddress(0)));

        // make sure the log only reads bytes between 0->HW (5)
        EasyMock.verify(log);

        // Test offset request from non-replica
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition.partitionId);
        Map<TopicAndPartition, OffsetRequest.PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition , new OffsetRequest.PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo,0,RequestOrResponse.OrdinaryConsumerId);
        ByteBuffer offsetRequestBB = TestUtils.createRequestByteBuffer(offsetRequest);

        EasyMock.reset(logManager);
        EasyMock.reset(replicaManager);

        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal());
        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
        Long[] leoarr = new Long[1];
        leoarr[0] = leo;
        EasyMock.expect(logManager.getOffsets(topicAndPartition, OffsetRequest.LatestTime, 1)).andReturn(leoarr);

        EasyMock.replay(replicaManager);
        EasyMock.replay(logManager);

        apis.handleOffsetRequest(new RequestChannel.Request( 0,
               5,
                offsetRequestBB,
                1,
                new InetSocketAddress(0)));
        BoundedByteBufferSend byteBufferSend = (BoundedByteBufferSend)(requestChannel.receiveResponse(0).responseSend);
        ByteBuffer offsetResponseBuffer = byteBufferSend.buffer;
        OffsetResponse offsetResponse = OffsetResponse.readFrom(offsetResponseBuffer);
        EasyMock.verify(replicaManager);
        EasyMock.verify(logManager);
        assertEquals(1, offsetResponse.partitionErrorAndOffsets.get(topicAndPartition).offsets.size());
        assertEquals(java.util.Optional.of(hw), offsetResponse.partitionErrorAndOffsets.get(topicAndPartition).offsets.get(0));
    }

    /**
     * The scenario for this test is that there is one topic, "test-topic", on broker "0" that has
     * one  partition with one follower replica on broker "1".  The leader replica on "0"
     * has HW of "5" and LEO of "20".  The follower on broker "1" has a local replica
     * with a HW matching the leader's ("5") and LEO of "15", meaning it's not in-sync
     * but is still in ISR (hasn't yet expired from ISR).
     *
     * When the follower from broker "1" fetches data, it should see data upto the log end offset ("20")
     */
    @Test
    public void  testReplicaSeesLeoWhenFetching() throws Exception {
        /* setup */
        long time = System.currentTimeMillis();
        long leo = 20;
        long hw = 5;

        Message messages = new Message("test-message".getBytes());

        int followerReplicaId = configs.get(1).brokerId;
        int followerLEO = 15;

        ZkClient zkClient = EasyMock.createNiceMock(ZkClient.class);
        EasyMock.expect(zkClient.exists(ZkUtils.ControllerEpochPath)).andReturn(false);
        EasyMock.replay(zkClient);

        Log log = EasyMock.createMock(Log.class);
        EasyMock.expect(log.logEndOffset()).andReturn(leo).anyTimes();
        EasyMock.expect(log.read(followerLEO, Integer.MAX_VALUE, null)).andReturn(new ByteBufferMessageSet(messages));
        EasyMock.replay(log);

        LogManager logManager = EasyMock.createMock(LogManager.class);
        EasyMock.expect(logManager.getLog(topic, 0)).andReturn(log).anyTimes();
        EasyMock.expect(logManager.config).andReturn(configs.get(0)).anyTimes();
        EasyMock.replay(logManager);

        ReplicaManager replicaManager = EasyMock.createMock(ReplicaManager.class);
        EasyMock.expect(replicaManager.config).andReturn(configs.get(0));
        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
        EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(ReplicaFetcherManager.class));
        EasyMock.expect(replicaManager.zkClient).andReturn(zkClient);
        EasyMock.replay(replicaManager);

        Partition partition = getPartitionWithAllReplicasInISR(topic, partitionId, time, configs.get(0).brokerId, log, hw, replicaManager);
        partition.getReplica(followerReplicaId).logEndOffset(followerLEO);

        EasyMock.reset(replicaManager);
        EasyMock.expect(replicaManager.config).andReturn(configs.get(0)).anyTimes();
        replicaManager.recordFollowerPosition(topic, partitionId, followerReplicaId, followerLEO);
        Replica firstReplica = null;
        for(Replica replica:partition.inSyncReplicas){
            if(replica.brokerId == configs.get(1).brokerId){
                firstReplica = replica;
                break;
            }
        }
        EasyMock.expect(replicaManager.getReplica(topic, partitionId, followerReplicaId)).andReturn(firstReplica);
        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal()).anyTimes();
        EasyMock.replay(replicaManager);

        KafkaController controller = EasyMock.createMock(KafkaController.class);

        RequestChannel requestChannel = new RequestChannel(2, 5);
        KafkaApis apis = new KafkaApis(requestChannel, replicaManager, zkClient, configs.get(0).brokerId, controller);

        /**
         * This fetch, coming from a replica, requests all data at offset "15".  Because the request is coming
         * from a follower, the leader should oblige and read beyond the HW.
         */
        FetchRequest bigFetch = new FetchRequest.FetchRequestBuilder()
                .replicaId(followerReplicaId)
                .addFetch(topic, partitionId, followerLEO, Integer.MAX_VALUE)
                .build();

        ByteBuffer fetchRequestBB = TestUtils.createRequestByteBuffer(bigFetch);

        // send the request
        apis.handleFetchRequest(new RequestChannel.Request(0,5, fetchRequestBB, 1, new InetSocketAddress(0)));

        /**
         * Make sure the log satisfies the fetch from a follower by reading data beyond the HW, mainly all bytes after
         * an offset of 15
         */
        EasyMock.verify(log);

        // Test offset request from replica
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition.partitionId);
        Map<TopicAndPartition, OffsetRequest.PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition , new OffsetRequest.PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo,0,RequestOrResponse.OrdinaryConsumerId);
        ByteBuffer offsetRequestBB = TestUtils.createRequestByteBuffer(offsetRequest);

        EasyMock.reset(logManager);
        EasyMock.reset(replicaManager);

        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal());
        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
        Long[] leoarr = new Long[1];
        leoarr[0] = leo;
        EasyMock.expect(logManager.getOffsets(topicAndPartition, OffsetRequest.LatestTime, 1)).andReturn(leoarr);

        EasyMock.replay(replicaManager);
        EasyMock.replay(logManager);

        apis.handleOffsetRequest(new RequestChannel.Request(1,
                 5,
                offsetRequestBB,
                 1,
                new InetSocketAddress(0)));
        BoundedByteBufferSend byteBufferSend = (BoundedByteBufferSend)(requestChannel.receiveResponse(1).responseSend);
        ByteBuffer offsetResponseBuffer = byteBufferSend.buffer;
        OffsetResponse offsetResponse = OffsetResponse.readFrom(offsetResponseBuffer);
        EasyMock.verify(replicaManager);
        EasyMock.verify(logManager);
        assertEquals(1, offsetResponse.partitionErrorAndOffsets.get(topicAndPartition).offsets.size());
        assertEquals(java.util.Optional.of(leo), offsetResponse.partitionErrorAndOffsets.get(topicAndPartition).offsets.get(0));
    }

    private Partition getPartitionWithAllReplicasInISR(String topic, int partitionId, long time, int leaderId,
                                                       Log localLog, long leaderHW, ReplicaManager replicaManager) {
        Partition partition = new Partition(topic, partitionId, 2, time, replicaManager);
        Replica leaderReplica = new Replica(leaderId, partition, time, 0, localLog);

        List<Replica> allReplicas = getFollowerReplicas(partition, leaderId, time) ;
        allReplicas.add(leaderReplica);
        allReplicas.forEach(r->partition.addReplicaIfNotExists(r));
        // set in sync replicas for this partition to all the assigned replicas
        partition.inSyncReplicas = allReplicas.stream().collect(Collectors.toSet());
        // set the leader and its hw and the hw update time
        partition.leaderReplicaIdOpt = leaderId;
        leaderReplica.highWatermark_(leaderHW);
        return partition;
    }

    private List<Replica> getFollowerReplicas(Partition partition,int leaderId,long time) {
        return configs.stream().filter(c->c.brokerId != leaderId).map(config ->
                new Replica(config.brokerId, partition, time,0L,null)).collect(Collectors.toList());
    }

}

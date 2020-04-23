package kafka.service;

import kafka.admin.CreateTopicCommand;
import kafka.api.LeaderAndIsrRequest;
import kafka.api.LeaderAndIsrResponse;
import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.controller.Callback;
import kafka.controller.ControllerChannelManager;
import kafka.controller.ControllerContext;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Pair;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;
import kafka.zk.ZooKeeperTestHarness;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class LeaderElectionTest extends ZooKeeperTestHarness {

    private static Logger logger = Logger.getLogger(LeaderElectionTest.class);

    int brokerId1 = 0;
    int brokerId2 = 1;

    int port1 ;
    int port2;

    Properties configProps1 = TestUtils.createBrokerConfig(brokerId1, port1);
    Properties configProps2 = TestUtils.createBrokerConfig(brokerId2, port2);
    List<KafkaServer> servers = new ArrayList<>();

    boolean staleControllerEpochDetected = false;
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // start both servers
        port1 = TestUtils.choosePort();
        port2 = TestUtils.choosePort();
        KafkaServer server1 = TestUtils.createServer(new KafkaConfig(configProps1));
        KafkaServer server2 = TestUtils.createServer(new KafkaConfig(configProps2));
        servers.add(server1);
        servers.add(server2);
    }

    @After
    public void tearDown() {
        for(KafkaServer server:servers){
            server.shutdown();
            Utils.rm(server.config.logDirs);
        }
        super.tearDown();
    }

    @Test
    public void testLeaderElectionAndEpoch() throws IOException, InterruptedException {
        // start 2 brokers
        String topic = "new-topic";
        int partitionId = 0;

        // create topic with 1 partition, 2 replicas, one on each broker
        CreateTopicCommand.createTopic(zkClient, topic, 1, 2, "0:1");

        // wait until leader is elected
        Integer leader1 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500,null);
        int leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logger.debug("leader Epoc: " + leaderEpoch1);
        logger.debug("Leader is elected to be: %s".format(leader1+""));
        assertTrue("Leader should get elected", leader1 != null);
        // NOTE: this is to avoid transient test failures
        assertTrue("Leader could be broker 0 or broker 1", ((leader1 == null?-1:leader1) == 0) || ((leader1== null?-1:leader1) == 1));
        assertEquals("First epoch value should be 0", 0, leaderEpoch1);

        // kill the server hosting the preferred replica
        servers.get(servers.size() - 1).shutdown();
        // check if leader moves to the other server
        Integer oldLeaderOpt1 = leader1;
        if(leader1 != null && leader1 == 1){
            oldLeaderOpt1 = null;
        }
        Integer leader2 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1500, oldLeaderOpt1);
        int leaderEpoch2 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logger.debug("Leader is elected to be: %s".format(leader1+""));
        logger.debug("leader Epoc: " + leaderEpoch2);
        assertEquals("Leader must move to broker 0", 0, leader2 == null?-1:leader2);
        if(leader1.equals(leader2))
            assertEquals("Second epoch value should be " + leaderEpoch1+1, leaderEpoch1+1, leaderEpoch2);
        else
            assertEquals("Second epoch value should be %d".format((leaderEpoch1+1) +"") , leaderEpoch1+1, leaderEpoch2);

        servers.get(servers.size() - 1).startup();
        servers.get(0).shutdown();
        Thread.sleep(TestUtils.tickTime);
        Integer oldLeaderOpt2 = leader2;
        if(leader2 != null && leader2 == 1){
            oldLeaderOpt2 = null;
        }
        Integer leader3 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1500, oldLeaderOpt2);
        int leaderEpoch3 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logger.debug("leader Epoc: " + leaderEpoch3);
        logger.debug("Leader is elected to be: %s".format(leader3+""));
        assertEquals("Leader must return to 1", 1, leader3+"");
        if(leader2.equals(leader3))
           assertEquals("Second epoch value should be " + leaderEpoch2, leaderEpoch2, leaderEpoch3);
        else
            assertEquals("Second epoch value should be %d".format((leaderEpoch2+1)+"") , leaderEpoch2+1, leaderEpoch3);
    }

    @Test
    public void testLeaderElectionWithStaleControllerEpoch() throws IOException, InterruptedException {
        // start 2 brokers
        String topic = "new-topic";
        int partitionId = 0;

        // create topic with 1 partition, 2 replicas, one on each broker
        CreateTopicCommand.createTopic(zkClient, topic, 1, 2, "0:1");

        // wait until leader is elected
        Integer leader1 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500,null);
        int leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logger.debug("leader Epoc: " + leaderEpoch1);
        logger.debug("Leader is elected to be: %s".format(leader1+""));
        assertTrue("Leader should get elected", leader1 != null);
        // NOTE: this is to avoid transient test failures
        assertTrue("Leader could be broker 0 or broker 1", ((leader1==null?-1:leader1) == 0) || ((leader1==null?-1:leader1) == 1));
        assertEquals("First epoch value should be 0", 0, leaderEpoch1);

        // start another controller
        int controllerId = 2;
        KafkaConfig controllerConfig = new KafkaConfig(TestUtils.createBrokerConfig(controllerId, TestUtils.choosePort()));
        List<Broker> brokers = servers.stream().map(s -> new Broker(s.config.brokerId, "localhost", s.config.port)).collect(Collectors.toList());
        ControllerContext controllerContext = new ControllerContext(zkClient, 6000);
        controllerContext.liveBrokers(brokers.stream().collect(Collectors.toSet())) ;
        ControllerChannelManager controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig);
        controllerChannelManager.startup();
        int staleControllerEpoch = 0;
        List<Integer> isr = new ArrayList<>();
        isr.add(brokerId1);
        isr.add(brokerId2);
        Map<Pair<String,Integer>, LeaderIsrAndControllerEpoch> leaderAndIsr = new HashMap<>();
        leaderAndIsr.put(new Pair<>(topic, partitionId),
                new LeaderIsrAndControllerEpoch(new LeaderAndIsrRequest.LeaderAndIsr(brokerId2,  isr), 2));
        Map<Pair<String,Integer>,LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfo = new HashMap<>();
        for(Map.Entry<Pair<String,Integer>, LeaderIsrAndControllerEpoch> entry : leaderAndIsr.entrySet()){
            Set<Integer> allReplicas = new HashSet<>();
            allReplicas.add(0);
            allReplicas.add(1);
            partitionStateInfo.put(entry.getKey(),new LeaderAndIsrRequest.PartitionStateInfo(entry.getValue(), allReplicas));
        }
        LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfo, brokers.stream().collect(Collectors.toSet()), controllerId,
                staleControllerEpoch, 0, "");

        controllerChannelManager.sendRequest(brokerId2, leaderAndIsrRequest, new Callback<RequestOrResponse>() {
            @Override
            public void onCallback(RequestOrResponse response) {
                staleControllerEpochCallback(response);
            }
        });
        long startTime = System.currentTimeMillis();
        boolean isS = false;
        while (true) {
            if (staleControllerEpochDetected == true) {
                isS = true;
                break;
            }
            if (System.currentTimeMillis() > startTime + 1000) {
                isS = false;
                break;
            }
            Thread.sleep(Math.min(1000,100L));
        }
        logger.info("isS res:"+isS);
        assertTrue("Stale controller epoch not detected by the broker", staleControllerEpochDetected);
        controllerChannelManager.shutdown();
    }

    private void staleControllerEpochCallback(RequestOrResponse response) {
        LeaderAndIsrResponse leaderAndIsrResponse = (LeaderAndIsrResponse)response;
        staleControllerEpochDetected = false;
        if(leaderAndIsrResponse.errorCode == ErrorMapping.StaleControllerEpochCode){
            staleControllerEpochDetected  = true;
        }
    }
}

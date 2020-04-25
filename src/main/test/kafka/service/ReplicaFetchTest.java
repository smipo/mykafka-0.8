package kafka.service;

import kafka.admin.CreateTopicCommand;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Lists;
import kafka.utils.TestUtils;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ReplicaFetchTest extends ZooKeeperTestHarness {

    List<Properties> props ;
    List<KafkaConfig> configs ;
    List<KafkaServer> brokers = new ArrayList<>();
    String topic1 = "foo";
    String topic2 = "bar";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        props = TestUtils.createBrokerConfigs(2);
        configs = props.stream().map(p -> new KafkaConfig(p)).collect(Collectors.toList());
        for(KafkaConfig config:configs){
            brokers.add(TestUtils.createServer(config));
        }
    }
    @After
    public void tearDown() throws Exception {
        brokers.forEach(x->x.shutdown());
        super.tearDown();
    }

    @Test
    public void testReplicaFetcherThread() throws Exception {
        int partition = 0;
        List<String> testMessageList1 = Lists.newArrayList("test1", "test2", "test3", "test4");
        List<String> testMessageList2 = Lists.newArrayList("test5", "test6", "test7", "test8");

        // create a topic and partition and await leadership
        for (String topic : Lists.newArrayList(topic1,topic2)) {
            StringBuilder sb = new StringBuilder();
            for(KafkaConfig config:configs){
                sb.append(config.brokerId+":");
            }
            CreateTopicCommand.createTopic(zkClient, topic, 1, 2, sb.toString().substring(0,sb.toString().length() - 1));
            TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000,null);
        }

        // send test messages to leader
        Producer<String, String> producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
                new StringEncoder(null),
                new StringEncoder(null));
        List<KeyedMessage<String,String>> messages = new ArrayList<>();
        for(String str:testMessageList1){
            messages.add(new KeyedMessage(topic1, str, str));
        }
        for(String str:testMessageList2){
            messages.add(new KeyedMessage(topic1, str, str));
        }
       // List<KeyedMessage<String,String>> messages = testMessageList1.stream().map(m -> new KeyedMessage(topic1, m, m)).collect(Collectors.toList());
       // List<KeyedMessage<String,String>> messages2 = testMessageList2.stream().map(m -> new KeyedMessage(topic1, m, m)).collect(Collectors.toList());
      //  messages.addAll(messages2);
        producer.send(messages);
        producer.close();
        long startTime = System.currentTimeMillis();
        boolean result;
        while (true) {
            if (logsMatch(partition)) {
                result = true;
                break;
            }
            if (System.currentTimeMillis() > startTime + 6000) {
                result = false;
                break;
            }
            Thread.sleep(Long.min(6000L,100L));
        }
        assertTrue("Broker logs should be identical", result);
    }
    public boolean logsMatch(int partition){
        boolean result = true;
        for (String topic : Lists.newArrayList(topic1, topic2)) {
            long expectedOffset = brokers.get(0).getLogManager().getLog(topic, partition).logEndOffset();

            for(KafkaServer kafkaServer:brokers){
                result = result && expectedOffset > 0 && (expectedOffset == kafkaServer.getLogManager().getLog(topic, partition).logEndOffset());
            }
        }
        return result;
    }
}

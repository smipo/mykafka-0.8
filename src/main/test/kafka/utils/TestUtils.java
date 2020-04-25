package kafka.utils;

import kafka.api.RequestOrResponse;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.Encoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;



public class TestUtils {

    private static Logger logger = Logger.getLogger(TestUtils.class);

    public static final String zookeeperConnect = "127.0.0.1:2182";

    public static Random random = new Random();

    public static int tickTime = 500;


    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        String ioDir = System.getProperty("java.io.tmpdir");
        File f = new File(ioDir, "kafka-" + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() throws IOException {
        File f = File.createTempFile("kafka", ".tmp");
        f.deleteOnExit();
        return f;
    }


    /**
     * Choose a number of random available ports
     */
    public static List<Integer> choosePorts(int count) throws IOException {
        List<ServerSocket> sockets = new ArrayList<>();
        for(int i = 0 ;i< count;i++)
            sockets.add(new ServerSocket(0));
        List<Integer> ports = sockets.stream().map(x->x.getLocalPort()).collect(Collectors.toList());
        for(ServerSocket socket:sockets){
            socket.close();
        }
        return ports;
    }

    /**
     * Choose an available port
     */
    public static int choosePort() throws IOException {
        return choosePorts(1).get(0);
    }


    /**
     * Create a kafka server instance with appropriate test settings
     * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
     * @param config The configuration of the server
     */
    public static KafkaServer createServer(KafkaConfig config) throws IOException, InterruptedException {
        KafkaServer server = new KafkaServer(config, System.currentTimeMillis());
        server.startup();
        return server;
    }
    /**
     * Create a test config for the given node id
     */
    public static Properties createBrokerConfig(int nodeId,int port) {
        Properties props = new Properties();
        props.put("broker.id", String.valueOf(nodeId));
        props.put("host.name", "localhost");
        props.put("port", String.valueOf(port));
        props.put("log.dir", TestUtils.tempDir().getAbsolutePath());
        props.put("log.flush.interval.messages", "1");
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("replica.socket.timeout.ms", "1500");
        return props;
    }

    public static Integer waitUntilLeaderIsElectedOrChanged(ZkClient zkClient,String topic,int partition,long timeoutMs, Integer oldLeaderOpt) throws IOException,InterruptedException{
        ReentrantLock leaderLock = new ReentrantLock();
        Condition leaderExistsOrChanged = leaderLock.newCondition();

        if(oldLeaderOpt == null)
            logger.info("Waiting for leader to be elected for partition [%s,%d]".format(topic, partition));
        else
            logger.info("Waiting for leader for partition [%s,%d] to be changed from old leader %d".format(topic, partition, oldLeaderOpt));
        leaderLock.lock();
        try{
            zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), new ZkUtils.LeaderExistsOrChangedListener(topic, partition, leaderLock, leaderExistsOrChanged, oldLeaderOpt, zkClient));
            leaderExistsOrChanged.await(timeoutMs, TimeUnit.MILLISECONDS);
            // check if leader is elected
            Integer leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
            if(leader == null){
                logger.error("Timing out after %d ms since leader is not elected for partition [%s,%d]"
                        .format(timeoutMs+"", topic, partition));
            }else{
                if(oldLeaderOpt == null)
                    logger.info("Leader %d is elected for partition [%s,%d]".format(leader+"", topic, partition));
                else
                    logger.info("Leader for partition [%s,%d] is changed from %d to %d".format(topic, partition, oldLeaderOpt, leader));

            }
            return leader;
        }finally {
            leaderLock.unlock();
        }
    }

    /**
     * Wait until the given condition is true or the given wait time ellapses
     */
    public static boolean waitUntilTrue(boolean condition,long waitTime) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (condition)
                return true;
            if (System.currentTimeMillis() > startTime + waitTime)
                return false;
            Thread.sleep(Math.min(waitTime,100L));
        }
    }

    /**
     * Create a test config for the given node id
     */
    public static List<Properties> createBrokerConfigs(int numConfigs) throws IOException {
        List<Properties> res = new ArrayList<>();
        List<Integer> list = choosePorts(numConfigs);
        for(int i = 0;i < list.size();i++){
            res.add(createBrokerConfig(i, list.get(i)));
        }
        return res;
    }

    /**
     * Create a producer for the given host and port
     */
    public static <K,V> Producer<K, V> createProducer(String brokerList, Encoder<V> encoder, Encoder<K> keyEncoder) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("send.buffer.bytes", "65536");
        props.put("connect.timeout.ms", "100000");
        props.put("reconnect.interval", "10000");
        props.put("serializer.class", encoder.getClass().getCanonicalName());
        props.put("key.serializer.class", keyEncoder.getClass().getCanonicalName());
        return new Producer<>(new ProducerConfig(props));
    }

    public static String getBrokerListStrFromConfigs(List<KafkaConfig> configs) {
        List<String> list = configs.stream().map(c -> c.hostName + ":" + c.port).collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        for(String str:list){
            sb.append(str+",");
        }
        return sb.toString().substring(0,sb.length() - 1);
    }

    public static ByteBuffer createRequestByteBuffer(RequestOrResponse request) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(request.sizeInBytes() + 2);
        byteBuffer.putShort(request.requestId);
        request.writeTo(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer;
    }
}

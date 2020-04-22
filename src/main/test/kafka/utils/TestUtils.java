package kafka.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class TestUtils {

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

}

package kafka.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestUtils {

    public static final String zookeeperConnect = "127.0.0.1:2182";

    public static Random random = new Random();


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

}

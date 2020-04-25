package kafka.zk;

import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class ZooKeeperTestHarness {

    public String zkConnect = TestUtils.zookeeperConnect;
    public ZkClient zkClient = null;
    public int zkConnectionTimeout = 6000;
    public int zkSessionTimeout = 6000;


    public void setUp() throws Exception {
        zkClient = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, new ZKStringSerializer());
    }


    public void tearDown()throws Exception  {
        zkClient.close();
    }
}

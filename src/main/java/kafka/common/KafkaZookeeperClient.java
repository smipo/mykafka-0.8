package kafka.common;

import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.atomic.AtomicReference;

public class KafkaZookeeperClient {

    private static AtomicReference<ZkClient> INSTANCE = new AtomicReference<>(null);

    public static ZkClient getZookeeperClient(ZkUtils.ZKConfig config) {
        // TODO: This cannot be a singleton since unit tests break if we do that
//    INSTANCE.compareAndSet(null, new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
//                                              ZKStringSerializer))
        INSTANCE.set(new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                new ZKStringSerializer()));
        return INSTANCE.get();
    }
}

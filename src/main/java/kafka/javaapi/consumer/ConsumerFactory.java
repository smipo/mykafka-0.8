package kafka.javaapi.consumer;

import kafka.consumer.ConsumerConfig;

import java.net.UnknownHostException;

public class ConsumerFactory {

    /**
     *  Create a ConsumerConnector
     *
     *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
     *                 connection string zookeeper.connect.
     */
    public static ConsumerConnector create(ConsumerConfig config) throws UnknownHostException {
        ZookeeperConsumerConnector consumerConnect = new ZookeeperConsumerConnector(config);
        return consumerConnect;
    }

    /**
     *  Create a ConsumerConnector
     *
     *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
     *                 connection string zookeeper.connect.
     */
    public static kafka.javaapi.consumer.ConsumerConnector createJavaConsumerConnector(ConsumerConfig config) throws UnknownHostException {
        kafka.javaapi.consumer.ZookeeperConsumerConnector consumerConnect = new kafka.javaapi.consumer.ZookeeperConsumerConnector(config);
        return consumerConnect;
    }
}

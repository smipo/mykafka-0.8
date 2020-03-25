package kafka.producer;

import kafka.common.InvalidConfigException;
import kafka.message.CompressionCodec;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;

import java.util.List;
import java.util.Properties;

public class ProducerConfig extends ZkUtils.ZKConfig {

    public ProducerConfig(Properties props){
        super(props);
        init();
    }

    private void init(){
        brokerList = Utils.getString(props, "broker.list", null);
        if(Utils.propertyExists(brokerList) && Utils.getString(props, "partitioner.class", null) != null)
            throw new InvalidConfigException("partitioner.class cannot be used when broker.list is set");
        numRetries = Utils.getInt(props, "num.retries", 0);

        /** If both broker.list and zk.connect options are specified, throw an exception */
        if(Utils.propertyExists(brokerList) && Utils.propertyExists(zkConnect))
            throw new InvalidConfigException("only one of broker.list and zk.connect can be specified");

        if(!Utils.propertyExists(zkConnect) && !Utils.propertyExists(brokerList))
            throw new InvalidConfigException("At least one of zk.connect or broker.list must be specified");

        compressionCodec = Utils.getCompressionCodec(props, "compression.codec");
        compressedTopics = Utils.getCSVList(Utils.getString(props, "compressed.topics", null));

        partitionerClass = Utils.getString(props, "partitioner.class", "kafka.producer.DefaultPartitioner");
        producerType = Utils.getString(props, "producer.type", "sync");

        zkReadRetries = Utils.getInt(props, "zk.read.num.retries", 3);

        cbkHandler = Utils.getString(props, "callback.handler", null);
        cbkHandlerProps = Utils.getProps(props, "callback.handler.props", null);
        eventHandler = Utils.getString(props, "event.handler", null);
        eventHandlerProps = Utils.getProps(props, "event.handler.props", null);
        serializerClass =  Utils.getString(props, "serializer.class", "kafka.serializer.DefaultEncoder");
    }

    /** For bypassing zookeeper based auto partition discovery, use this config   *
     *  to pass in static broker and per-broker partition information. Format-    *
     *  brokerid1:host1:port1, brokerid2:host2:port2*/
    public String brokerList ;


    /**
     * If DefaultEventHandler is used, this specifies the number of times to
     * retry if an error is encountered during send. Currently, it is only
     * appropriate when broker.list points to a VIP. If the zk.connect option
     * is used instead, this will not have any effect because with the zk-based
     * producer, brokers are not re-selected upon retry. So retries would go to
     * the same (potentially still down) broker. (KAFKA-253 will help address
     * this.)
     */
    public int numRetries ;



    /** the partitioner class for partitioning events amongst sub-topics */
    public String partitionerClass ;

    /** this parameter specifies whether the messages are sent asynchronously *
     * or not. Valid values are - async for asynchronous send                 *
     *                            sync for synchronous send                   */
    public  String producerType ;

    /**
     * This parameter allows you to specify the compression codec for all data generated *
     * by this producer. The default is NoCompressionCodec
     */
    public CompressionCodec compressionCodec ;

    /** This parameter allows you to set whether compression should be turned *
     *  on for particular topics
     *
     *  If the compression codec is anything other than NoCompressionCodec,
     *
     *    Enable compression only for specified topics if any
     *
     *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
     *
     *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
     */
    public List<String> compressedTopics ;

    /**
     * The producer using the zookeeper software load balancer maintains a ZK cache that gets
     * updated by the zookeeper watcher listeners. During some events like a broker bounce, the
     * producer ZK cache can get into an inconsistent state, for a small time period. In this time
     * period, it could end up picking a broker partition that is unavailable. When this happens, the
     * ZK cache needs to be updated.
     * This parameter specifies the number of times the producer attempts to refresh this ZK cache.
     */
    public int zkReadRetries ;

    /** the callback handler for one or multiple events */
    public String cbkHandler ;

    /** properties required to initialize the callback handler */
    public Properties cbkHandlerProps ;

    /** the handler for events */
    public String eventHandler ;

    /** properties required to initialize the callback handler */
    public Properties eventHandlerProps;

    public String serializerClass;

}

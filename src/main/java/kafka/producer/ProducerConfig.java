package kafka.producer;

import kafka.common.InvalidConfigException;
import kafka.message.CompressionCodec;
import kafka.message.CompressionFactory;
import kafka.producer.async.AsyncProducerConfig;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.List;
import java.util.Properties;

public class ProducerConfig extends AsyncProducerConfig {

    public void validate(ProducerConfig config) {
        validateBatchSize(config.batchNumMessages, config.queueBufferingMaxMessages);
        validateProducerType(config.producerType);
    }


    public void  validateBatchSize(int batchSize, int queueSize) {
        if (batchSize > queueSize)
            throw new InvalidConfigException("Batch size = " + batchSize + " can't be larger than queue size = " + queueSize);
    }

    public void  validateProducerType(String producerType) {
        if(!producerType.equals("sync") && !producerType.equals("async")){
            throw new InvalidConfigException("Invalid value " + producerType + " for producer.type, valid values are sync/async");
        }
    }

    public ProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }
    public ProducerConfig(VerifiableProperties props){
        super(props);
        init();
    }

    private void init(){
        brokerList = props.getString("metadata.broker.list");


        String prop = props.getString("compression.codec", "none");
        try {
            compressionCodec = CompressionFactory.getCompressionCodec(Integer.parseInt(prop));
        }
        catch (NumberFormatException nfe){
            compressionCodec =  CompressionFactory.getCompressionCodec(prop);
        }
        compressedTopics = Utils.getCSVList(props.getString("compressed.topics", null));

        partitionerClass = props.getString("partitioner.class", "kafka.producer.DefaultPartitioner");
        producerType = props.getString("producer.type", "sync");
        retryBackoffMs = props.getInt("retry.backoff.ms", 100);
        messageSendMaxRetries = props.getInt("message.send.max.retries", 3);
        topicMetadataRefreshIntervalMs = props.getInt("topic.metadata.refresh.interval.ms", 600000);
        clientId = props.getString("client.id", "producer");
        validate(this);
    }

    /** For bypassing zookeeper based auto partition discovery, use this config   *
     *  to pass in static broker and per-broker partition information. Format-    *
     *  brokerid1:host1:port1, brokerid2:host2:port2*/
    public String brokerList ;




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

    /** The leader may be unavailable transiently, which can fail the sending of a message.
     *  This property specifies the number of retries when such failures occur.
     */
    public int messageSendMaxRetries ;

    /** Before each retry, the producer refreshes the metadata of relevant topics. Since leader
     * election takes a bit of time, this property specifies the amount of time that the producer
     * waits before refreshing the metadata.
     */
    public  int retryBackoffMs ;

    /**
     * The producer generally refreshes the topic metadata from brokers when there is a failure
     * (partition missing, leader not available...). It will also poll regularly (default: every 10min
     * so 600000ms). If you set this to a negative value, metadata will only get refreshed on failure.
     * If you set this to zero, the metadata will get refreshed after each message sent (not recommended)
     * Important note: the refresh happen only AFTER the message is sent, so if the producer never sends
     * a message the metadata is never refreshed
     */
    public int topicMetadataRefreshIntervalMs;

    public String clientId ;

}

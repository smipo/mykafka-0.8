package kafka.consumer;

import kafka.api.OffsetRequest;
import kafka.common.Config;
import kafka.common.InvalidConfigException;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZkUtils;

import java.util.Properties;

public class ConsumerConfig extends ZkUtils.ZKConfig {

    public static int  RefreshMetadataBackoffMs = 200;
    public static int SocketTimeout = 30 * 1000;
    public static int SocketBufferSize = 64*1024;
    public static int FetchSize = 1024 * 1024;
    public static int MaxFetchSize = 10*FetchSize;
    public static int DefaultFetcherBackoffMs = 1000;
    public static boolean AutoCommit = true;
    public static int AutoCommitInterval   = 10 * 1000;
    public static int MaxQueuedChunks = 10;
    public static int MaxRebalanceRetries = 4;
    public static String AutoOffsetReset = OffsetRequest.SmallestTimeString;
    public static int ConsumerTimeoutMs = -1;
    public static String MirrorTopicsWhitelist = "";
    public static String MirrorTopicsBlacklist = "";
    public static int MirrorConsumerNumThreads = 1;

    public static String MirrorTopicsWhitelistProp = "mirror.topics.whitelist";
    public static String MirrorTopicsBlacklistProp = "mirror.topics.blacklist";
    public static String MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads";
    public static int MinFetchBytes = 1;
    public static int MaxFetchWaitMs = 100;


    public static void validate(ConsumerConfig config) {
        validateClientId(config.clientId);
        validateGroupId(config.groupId);
        validateAutoOffsetReset(config.autoOffsetReset);
    }

    public static void validateClientId(String clientId) {
        Config.validateChars("client.id", clientId);
    }

    public static void validateGroupId(String groupId) {
        Config.validateChars("group.id", groupId);
    }

    public static void validateAutoOffsetReset(String autoOffsetReset) {
        if(autoOffsetReset.equals(OffsetRequest.SmallestTimeString)){

        }else if(autoOffsetReset.equals(OffsetRequest.LargestTimeString)){

        }else{
            throw new InvalidConfigException("Wrong value " + autoOffsetReset + " of auto.offset.reset in ConsumerConfig; " +
                    "Valid values are " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString);
        }
    }

    public ConsumerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }

    public ConsumerConfig(VerifiableProperties props){
        super(props);

        groupId = props.getString( "group.id");
        consumerId = props.getString("consumer.id", null);
        socketTimeoutMs = props.getInt( "socket.timeout.ms", SocketTimeout);
        socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize);
        autoCommitIntervalMs = props.getInt( "autocommit.interval.ms", AutoCommitInterval);
        rebalanceBackoffMs = props.getInt( "rebalance.backoff.ms", zkSyncTimeMs);
        autoOffsetReset = props.getString( "autooffset.reset", AutoOffsetReset);
        consumerTimeoutMs = props.getInt( "consumer.timeout.ms", ConsumerTimeoutMs);
        fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize);
        queuedMaxMessages = props.getInt("queued.max.message.chunks", MaxQueuedChunks);
        autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit);
        rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries);
        fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes);
        fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs);
        refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshMetadataBackoffMs);
        clientId = props.getString("client.id", groupId);
        validate(this);
    }

    /** a string that uniquely identifies a set of consumers within the same consumer group */
    public String groupId ;

    /** consumer id: generated automatically if not set.
     *  Set this explicitly for only testing purpose. */
    public String consumerId ;

    /** the socket timeout for network requests */
    public int socketTimeoutMs;
    /** the socket receive buffer for network requests */
    public int socketReceiveBufferBytes ;


    /** the frequency in ms that the consumer offsets are committed to zookeeper */
    public int autoCommitIntervalMs ;


    /** backoff time between retries during rebalance */
    public int rebalanceBackoffMs;

    /* what to do if an offset is out of range.
       smallest : automatically reset the offset to the smallest offset
       largest : automatically reset the offset to the largest offset
       anything else: throw exception to the consumer */
    public String autoOffsetReset ;

    /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
    public int consumerTimeoutMs;


    /** the number of byes of messages to attempt to fetch */
    public int fetchMessageMaxBytes ;

    /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
    public  boolean autoCommitEnable;

    /** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
    public int queuedMaxMessages ;

    /** max number of retries during rebalance */
    public int rebalanceMaxRetries ;

    /** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
    public int fetchMinBytes;

    /** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
    public int fetchWaitMaxMs;

    /** backoff time to refresh the leader of a partition after it loses the current leader */
    public int refreshLeaderBackoffMs;

    /**
     * Client id is specified by the kafka consumer client, used to distinguish different clients
     */
    public  String clientId ;
}

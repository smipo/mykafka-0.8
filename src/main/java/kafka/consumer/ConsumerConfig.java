package kafka.consumer;

import kafka.api.OffsetRequest;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;

import java.util.Properties;

public class ConsumerConfig extends ZkUtils.ZKConfig {

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


    public ConsumerConfig(Properties props){
        super(props);

        groupId = Utils.getString(props, "groupid");
        if (Utils.getString(props, "consumerid", null) != null)   consumerId = Utils.getString(props, "consumerid");
        socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", SocketTimeout);
        socketBufferSize = Utils.getInt(props, "socket.buffersize", SocketBufferSize);
        fetchSize = Utils.getInt(props, "fetch.size", FetchSize);
        fetcherBackoffMs = Utils.getInt(props, "fetcher.backoff.ms", DefaultFetcherBackoffMs);
        autoCommit = Utils.getBoolean(props, "autocommit.enable", AutoCommit);
        autoCommitIntervalMs = Utils.getInt(props, "autocommit.interval.ms", AutoCommitInterval);
        maxQueuedChunks = Utils.getInt(props, "queuedchunks.max", MaxQueuedChunks);
        maxRebalanceRetries = Utils.getInt(props, "rebalance.retries.max", MaxRebalanceRetries);
        rebalanceBackoffMs = Utils.getInt(props, "rebalance.backoff.ms", zkSyncTimeMs);
        autoOffsetReset = Utils.getString(props, "autooffset.reset", AutoOffsetReset);
        consumerTimeoutMs = Utils.getInt(props, "consumer.timeout.ms", ConsumerTimeoutMs);
        enableShallowIterator = Utils.getBoolean(props, "shallowiterator.enable", false);
    }

    /** a string that uniquely identifies a set of consumers within the same consumer group */
    String groupId ;

    /** consumer id: generated automatically if not set.
     *  Set this explicitly for only testing purpose. */
    String consumerId = null;

    /** the socket timeout for network requests */
    int socketTimeoutMs;

    /** the socket receive buffer for network requests */
    int socketBufferSize ;

    /** the number of byes of messages to attempt to fetch */
    int fetchSize ;

    /** to avoid repeatedly polling a broker node which has no new data
     we will backoff every time we get an empty set from the broker*/
    long fetcherBackoffMs ;

    /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
    boolean autoCommit ;

    /** the frequency in ms that the consumer offsets are committed to zookeeper */
    int autoCommitIntervalMs ;

    /** max number of messages buffered for consumption */
    int maxQueuedChunks ;

    /** max number of retries during rebalance */
    int maxRebalanceRetries ;

    /** backoff time between retries during rebalance */
    int rebalanceBackoffMs;

    /* what to do if an offset is out of range.
       smallest : automatically reset the offset to the smallest offset
       largest : automatically reset the offset to the largest offset
       anything else: throw exception to the consumer */
    String autoOffsetReset ;

    /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
    int consumerTimeoutMs;

    /** Use shallow iterator over compressed messages directly. This feature should be used very carefully.
     *  Typically, it's only used for mirroring raw messages from one kafka cluster to another to save the
     *  overhead of decompression.
     *  */
    boolean enableShallowIterator ;
}

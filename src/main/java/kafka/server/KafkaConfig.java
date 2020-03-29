package kafka.server;

import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.message.MessageSet;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZkUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kafka.utils.Preconditions.*;

public class KafkaConfig extends ZkUtils.ZKConfig {

 public KafkaConfig(VerifiableProperties props){
  super(props);
  init();
 }

 public KafkaConfig(Properties originalProps) {
  this(new VerifiableProperties(originalProps));
  props.verify();
  init();
 }

 private void init(){
  brokerId = props.getIntInRange("broker.id", 0, Integer.MAX_VALUE);
  messageMaxBytes = props.getIntInRange("message.max.bytes", 1000000 + MessageSet.LogOverhead, 0, Integer.MAX_VALUE);
  numNetworkThreads = props.getIntInRange("num.network.threads", 3, 1, Integer.MAX_VALUE);
  numIoThreads = props.getIntInRange("num.io.threads", 8, 1, Integer.MAX_VALUE);
  queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, 1, Integer.MAX_VALUE);
  port = props.getInt("port", 6667);
  hostName = props.getString("host.name", null);
  socketSendBufferBytes = props.getInt("socket.send.buffer.bytes", 100*1024);
  socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 100*1024);
  socketRequestMaxBytes = props.getIntInRange("socket.request.max.bytes", 100*1024*1024, 1, Integer.MAX_VALUE);
  numPartitions = props.getIntInRange("num.partitions", 1, 1, Integer.MAX_VALUE);
  logDirs = Utils.parseCsvList(props.getString("log.dirs", props.getString("log.dir", "/tmp/kafka-logs")));
  checkArgument(logDirs.size() > 0);
  logSegmentBytes = props.getIntInRange("log.segment.bytes", 1*1024*1024*1024, Message.MinHeaderSize, Integer.MAX_VALUE);
  logSegmentBytesPerTopicMap = props.getMap("log.segment.bytes.per.topic");
  logRollHours = props.getIntInRange("log.roll.hours", 24*7, 1, Integer.MAX_VALUE);
  logRollHoursPerTopicMap = props.getMap("log.roll.hours.per.topic");
  logRetentionHours = props.getIntInRange("log.retention.hours", 24*7, 1, Integer.MAX_VALUE);
  logRetentionHoursPerTopicMap = props.getMap("log.retention.hours.per.topic");
  logRetentionBytes = props.getLong("log.retention.bytes", -1);
  logRetentionBytesPerTopicMap = props.getMap("log.retention.bytes.per.topic");
  logCleanupIntervalMins = props.getIntInRange("log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
  logIndexSizeMaxBytes = props.getIntInRange("log.index.size.max.bytes", 10*1024*1024, 4, Integer.MAX_VALUE);
  logIndexIntervalBytes = props.getIntInRange("log.index.interval.bytes", 4096, 0, Integer.MAX_VALUE);
  logFlushIntervalMessages = props.getIntInRange("log.flush.interval.messages", 10000, 1,Integer.MAX_VALUE);
  logFlushIntervalMsPerTopicMap = props.getMap("log.flush.interval.ms.per.topic");
  logFlushSchedulerIntervalMs = props.getInt("log.flush.scheduler.interval.ms",  3000);
  logFlushIntervalMs = props.getInt("log.flush.interval.ms", logFlushSchedulerIntervalMs);
  autoCreateTopicsEnable = props.getBoolean("auto.create.topics.enable", true);
  controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000);
  controllerMessageQueueSize= props.getInt("controller.message.queue.size", 10);
  defaultReplicationFactor = props.getInt("default.replication.factor", 1);
  replicaLagTimeMaxMs = props.getLong("replica.lag.time.max.ms", 10000);
  replicaLagMaxMessages = props.getLong("replica.lag.max.messages", 4000);
  replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfig.SocketTimeout);
  replicaSocketReceiveBufferBytes = props.getInt("replica.socket.receive.buffer.bytes", ConsumerConfig.SocketBufferSize);
  replicaFetchMaxBytes = props.getInt("replica.fetch.max.bytes", ConsumerConfig.FetchSize);
  replicaFetchWaitMaxMs = props.getInt("replica.fetch.wait.max.ms", 500);
  replicaFetchMinBytes = props.getInt("replica.fetch.min.bytes", 1);
  numReplicaFetchers = props.getInt("num.replica.fetchers", 1);
  replicaHighWatermarkCheckpointIntervalMs = props.getLong("replica.high.watermark.checkpoint.interval.ms", 5000L);
  fetchPurgatoryPurgeIntervalRequests = props.getInt("fetch.purgatory.purge.interval.requests", 10000);
  controlledShutdownMaxRetries = props.getInt("controlled.shutdown.max.retries", 3);
  producerPurgatoryPurgeIntervalRequests = props.getInt("producer.purgatory.purge.interval.requests", 10000);
  controlledShutdownRetryBackoffMs = props.getInt("controlled.shutdown.retry.backoff.ms", 5000);
  controlledShutdownEnable = props.getBoolean("controlled.shutdown.enable", false);
 }
 /*********** General Configuration ***********/

 /* the broker id for this server */
 public int brokerId;

 /* the maximum size of message that the server can receive */
 public int messageMaxBytes ;

 /* the number of network threads that the server uses for handling network requests */
 public int numNetworkThreads;

 /* the number of io threads that the server uses for carrying out network requests */
 public int numIoThreads;

 /* the number of queued requests allowed before blocking the network threads */
 public int queuedMaxRequests ;

 /*********** Socket Server Configuration ***********/

 /* the port to listen and accept connections on */
 public int port ;

 /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
  * it will bind to all interfaces, and publish one to ZK */
 public String hostName ;

 /* the SO_SNDBUFF buffer of the socket sever sockets */
 public int socketSendBufferBytes ;

 /* the SO_RCVBUFF buffer of the socket sever sockets */
 public int socketReceiveBufferBytes ;

 /* the maximum number of bytes in a socket request */
 public int socketRequestMaxBytes ;

 /*********** Log Configuration ***********/

 /* the default number of log partitions per topic */
 public int numPartitions ;

 /* the directories in which the log data is kept */
 public List<String> logDirs;

 /* the maximum size of a single log file */
 public int logSegmentBytes;

 /* the maximum size of a single log file for some specific topic */
 public Map<String,String> logSegmentBytesPerTopicMap ;

 /* the maximum time before a new log segment is rolled out */
 public int logRollHours;

 /* the number of hours before rolling out a new log segment for some specific topic */
 public Map<String,String> logRollHoursPerTopicMap ;

 /* the number of hours to keep a log file before deleting it */
 public int logRetentionHours;

 /* the number of hours to keep a log file before deleting it for some specific topic*/
 public Map<String,String> logRetentionHoursPerTopicMap ;

 /* the maximum size of the log before deleting it */
 public long logRetentionBytes ;

 /* the maximum size of the log for some specific topic before deleting it */
 public Map<String,String> logRetentionBytesPerTopicMap ;

 /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
 public int logCleanupIntervalMins;

 /* the maximum size in bytes of the offset index */
 public int logIndexSizeMaxBytes ;

 /* the interval with which we add an entry to the offset index */
 public int logIndexIntervalBytes ;

 /* the number of messages accumulated on a log partition before messages are flushed to disk */
 public int logFlushIntervalMessages;

 /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
 public Map<String,String> logFlushIntervalMsPerTopicMap;

 /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
 public int logFlushSchedulerIntervalMs;

 /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
 public int logFlushIntervalMs ;

 /* enable auto creation of topic on the server */
 public boolean autoCreateTopicsEnable;

 /*********** Replication configuration ***********/

 /* the socket timeout for controller-to-broker channels */
 public int controllerSocketTimeoutMs ;

 /* the buffer size for controller-to-broker-channels */
 public int controllerMessageQueueSize;

 /* default replication factors for automatically created topics */
 public int defaultReplicationFactor ;

 /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
 public long replicaLagTimeMaxMs ;

 /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
 public long replicaLagMaxMessages ;

 /* the socket timeout for network requests */
 public int replicaSocketTimeoutMs ;

 /* the socket receive buffer for network requests */
 public int replicaSocketReceiveBufferBytes ;

 /* the number of byes of messages to attempt to fetch */
 public int replicaFetchMaxBytes ;

 /* max wait time for each fetcher request issued by follower replicas*/
 public int replicaFetchWaitMaxMs ;

 /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
 public int replicaFetchMinBytes ;

 /* number of fetcher threads used to replicate messages from a source broker.
  * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
 public int numReplicaFetchers ;

 /* the frequency with which the high watermark is saved out to disk */
 public long replicaHighWatermarkCheckpointIntervalMs ;

 /* the purge interval (in number of requests) of the fetch request purgatory */
 public int fetchPurgatoryPurgeIntervalRequests ;

 /* the purge interval (in number of requests) of the producer request purgatory */
 public int producerPurgatoryPurgeIntervalRequests ;

 /*********** Controlled shutdown configuration ***********/

 /** Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens */
 public int controlledShutdownMaxRetries;

 /** Before each retry, the system needs time to recover from the state that caused the previous failure (Controller
  * fail over, replica lag etc). This config determines the amount of time to wait before retrying. */
 public int controlledShutdownRetryBackoffMs ;

 /* enable controlled shutdown of the server */
 public boolean controlledShutdownEnable ;
}

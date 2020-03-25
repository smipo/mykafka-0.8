package kafka.server;

import kafka.message.Message;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;

import java.util.Map;
import java.util.Properties;

public class KafkaConfig extends ZkUtils.ZKConfig {

 public KafkaConfig(Properties props){
  super(props);
  init();
 }

 public void init(){
  port = Utils.getInt(props, "port", 6667);
  hostName = Utils.getString(props, "hostname", null);
  brokerId = Utils.getInt(props, "brokerid");
  socketSendBuffer = Utils.getInt(props, "socket.send.buffer", 100*1024);
  socketReceiveBuffer = Utils.getInt(props, "socket.receive.buffer", 100*1024);
  maxSocketRequestSize = Utils.getIntInRange(props, "max.socket.request.bytes", 100*1024*1024, 1, Integer.MAX_VALUE);
  maxMessageSize = Utils.getIntInRange(props, "max.message.size", 1000000, 0, Integer.MAX_VALUE);
  numThreads = Utils.getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors(), 1, Integer.MAX_VALUE);
  monitoringPeriodSecs = Utils.getIntInRange(props, "monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
  numPartitions = Utils.getIntInRange(props, "num.partitions", 1, 1, Integer.MAX_VALUE);
  logDir = Utils.getString(props, "log.dir");
  logFileSize = Utils.getIntInRange(props, "log.file.size", 1*1024*1024*1024, Message.MinHeaderSize, Integer.MAX_VALUE);
  logFileSizeMap = Utils.getTopicFileSize(Utils.getString(props, "topic.log.file.size", ""));
  logRollHours = Utils.getIntInRange(props, "log.roll.hours", 24*7, 1, Integer.MAX_VALUE);
  logRollHoursMap = Utils.getTopicRollHours(Utils.getString(props, "topic.log.roll.hours", ""));
  logRetentionHours = Utils.getIntInRange(props, "log.retention.hours", 24*7, 1, Integer.MAX_VALUE);
  logRetentionHoursMap = Utils.getTopicRetentionHours(Utils.getString(props, "topic.log.retention.hours", ""));
  logRetentionSize = Utils.getLong(props, "log.retention.size", -1);
  logRetentionSizeMap = Utils.getTopicRetentionSize(Utils.getString(props, "topic.log.retention.size", ""));
  logCleanupIntervalMinutes = Utils.getIntInRange(props, "log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
  enableZookeeper = Utils.getBoolean(props, "enable.zookeeper", true);
  flushInterval = Utils.getIntInRange(props, "log.flush.interval", 500, 1, Integer.MAX_VALUE);
  flushIntervalMap = Utils.getTopicFlushIntervals(Utils.getString(props, "topic.flush.intervals.ms", ""));
  flushSchedulerThreadRate = Utils.getInt(props, "log.default.flush.scheduler.interval.ms",  3000);
  defaultFlushIntervalMs = Utils.getInt(props, "log.default.flush.interval.ms", flushSchedulerThreadRate);
  topicPartitionsMap = Utils.getTopicPartitions(Utils.getString(props, "topic.partition.count.map", ""));
  maxTopicNameLength = Utils.getIntInRange(props, "max.topic.name.length", 255, 1, Integer.MAX_VALUE);
 }

 /* the port to listen and accept connections on */
 public int port ;

 /* hostname of broker. If not set, will pick up from the value returned from getLocalHost. If there are multiple interfaces getLocalHost may not be what you want. */
 public String hostName ;

 /* the broker id for this server */
 public int brokerId ;

 /* the SO_SNDBUFF buffer of the socket sever sockets */
 public int socketSendBuffer ;

 /* the SO_RCVBUFF buffer of the socket sever sockets */
 public int socketReceiveBuffer ;

 /* the maximum number of bytes in a socket request */
 public int maxSocketRequestSize ;

 /* the maximum size of message that the server can receive */
 public int maxMessageSize ;

 /* the number of worker threads that the server uses for handling all client requests*/
 public int numThreads ;

 /* the interval in which to measure performance statistics */
 public int monitoringPeriodSecs ;

 /* the default number of log partitions per topic */
 public int numPartitions ;

 /* the directory in which the log data is kept */
 public String logDir ;

 /* the maximum size of a single log file */
 public int logFileSize ;

 /* the maximum size of a single log file for some specific topic */
 public Map<String,Integer> logFileSizeMap ;

 /* the maximum time before a new log segment is rolled out */
 public int logRollHours ;

 /* the number of hours before rolling out a new log segment for some specific topic */
 public Map<String,Integer> logRollHoursMap ;

 /* the number of hours to keep a log file before deleting it */
 public int logRetentionHours ;

 /* the number of hours to keep a log file before deleting it for some specific topic*/
 public Map<String,Integer> logRetentionHoursMap ;

 /* the maximum size of the log before deleting it */
 public long logRetentionSize ;

 /* the maximum size of the log for some specific topic before deleting it */
 public Map<String,Integer> logRetentionSizeMap ;

 /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
 public int logCleanupIntervalMinutes ;

 /* enable zookeeper registration in the server */
 public boolean enableZookeeper;

 /* the number of messages accumulated on a log partition before messages are flushed to disk */
 public int flushInterval;

 /* the maximum time in ms that a message in selected topics is kept in memory before flushed to disk, e.g., topic1:3000,topic2: 6000  */
 public Map<String,Integer> flushIntervalMap ;

 /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
 public int flushSchedulerThreadRate ;

 /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
 public int defaultFlushIntervalMs ;

 /* the number of partitions for selected topics, e.g., topic1:8,topic2:16 */
 public Map<String,Integer> topicPartitionsMap ;

 /* the maximum length of topic name*/
 public int maxTopicNameLength;
}

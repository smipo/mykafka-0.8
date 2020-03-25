package kafka.log;

import kafka.api.OffsetRequest;
import kafka.common.InvalidPartitionException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaZooKeeper;
import kafka.utils.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * The guy who creates and hands out logs
 */
public class LogManager {

    private static Logger logger = Logger.getLogger(LogManager.class);

    KafkaConfig config;
    private KafkaScheduler scheduler;
    private long milliseconds;
    long logRollDefaultIntervalMs;
    long logCleanupIntervalMs;
    long logCleanupDefaultAgeMs;
    boolean needRecovery;

    public LogManager(KafkaConfig config,
                        KafkaScheduler scheduler,
                        long milliseconds,
                        long logRollDefaultIntervalMs,
                        long logCleanupIntervalMs,
                        long logCleanupDefaultAgeMs,
                        boolean needRecovery) throws IOException{
        this.config = config;
        this.scheduler = scheduler;
        this.milliseconds = milliseconds;
        this.logRollDefaultIntervalMs = logRollDefaultIntervalMs;
        this.logCleanupIntervalMs = logCleanupIntervalMs;
        this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
        this.needRecovery = needRecovery;

        logDir = new File(config.logDir);
        numPartitions = config.numPartitions;
        logFileSizeMap = config.logFileSizeMap;
        flushInterval = config.flushInterval;
        topicPartitionsMap = config.topicPartitionsMap;
        if (config.enableZookeeper) startupLatch = new CountDownLatch(1);
        logFlushIntervalMap = config.flushIntervalMap;
        logRetentionSizeMap = config.logRetentionSizeMap;
        logRetentionMsMap = getMsMap(config.logRetentionHoursMap);
        logRollMsMap = getMsMap(config.logRollHoursMap);
        topicNameValidator = new TopicNameValidator(config);
        if(!logDir.exists()) {
            logger.info("No log directory found, creating '" + logDir.getAbsolutePath() + "'");
            logDir.mkdirs();
        }
        if(!logDir.isDirectory() || !logDir.canRead())
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.");
        File[] subDirs = logDir.listFiles();
        if(subDirs != null) {
            for(File dir : subDirs) {
                if(!dir.isDirectory()) {
                    logger.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?");
                } else {
                    logger.info("Loading log '" + dir.getName() + "'");
                    String topic = Utils.getTopicPartition(dir.getName()).getKey();
                    Long rollIntervalMs = logRollMsMap.getOrDefault(topic,this.logRollDefaultIntervalMs);
                    Integer maxLogFileSize = logFileSizeMap.getOrDefault(topic,config.logFileSize);
                    Log log = new Log(dir, milliseconds, maxLogFileSize, config.maxMessageSize, flushInterval, rollIntervalMs, needRecovery);
                    Pair<String, Integer> topicPartion = Utils.getTopicPartition(dir.getName());
                    logs.putIfNotExists(topicPartion.getKey(), new Pool<Integer, Log>());
                    Pool<Integer,Log> parts = logs.get(topicPartion.getKey());
                    parts.put(topicPartion.getValue(), log);
                }
            }
        }

        /* Schedule the cleanup task to delete old logs */
        if(scheduler != null) {
            logger.info("starting log cleaner every " + logCleanupIntervalMs + " ms");
            scheduler.scheduleWithRate(new Runnable() {
                @Override
                public void run() {
                    try{
                        cleanupLogs();
                    }catch (IOException e){
                        logger.error("cleanup log Error:",e);
                    }

                }
            }, 60 * 1000, logCleanupIntervalMs);
        }

        if(config.enableZookeeper) {
            kafkaZookeeper = new KafkaZooKeeper(config, this);
            kafkaZookeeper.startup();
        }
    }


    File logDir;
    private int numPartitions ;
    private Map<String,Integer> logFileSizeMap ;
    private int flushInterval;
    private Map<String,Integer> topicPartitionsMap ;
    private Object logCreationLock = new Object();
    private Random random = new Random();
    private KafkaZooKeeper kafkaZookeeper = null;
    private CountDownLatch startupLatch = null;
    private KafkaScheduler logFlusherScheduler = new KafkaScheduler(1, "kafka-logflusher-", false);
    private TopicNameValidator topicNameValidator;
    private Map<String,Integer> logFlushIntervalMap;
    private Map<String,Integer> logRetentionSizeMap ;
    private Map<String, Long>  logRetentionMsMap;
    private Map<String, Long>  logRollMsMap;

    /* Initialize a log for each subdirectory of the main log directory */
    private kafka.utils.Pool<String, Pool<Integer,Log>> logs = new Pool<>();

    private Map<String, Long> getMsMap(Map<String, Integer> hoursMap)  {
        Map<String, Long> ret = new HashMap<>();
        for (Map.Entry<String, Integer> entry : hoursMap.entrySet()) {
            String topic = entry.getKey();
            Integer hour = entry.getValue();
            ret.put(topic, hour * 60 * 60 * 1000L);
        }
        return ret;
    }


    /**
     *  Register this broker in ZK for the first time.
     */
    public void startup() {
        if(config.enableZookeeper) {
            kafkaZookeeper.registerBrokerInZk();
            Iterator<String> iterator = getAllTopics();
            while (iterator.hasNext()){
                String topic = iterator.next();
                kafkaZookeeper.registerTopicInZk(topic);
            }
            startupLatch.countDown();
        }
        logger.info("Starting log flusher every " + config.flushSchedulerThreadRate + " ms with the following overrides " + logFlushIntervalMap);
        logFlusherScheduler.scheduleWithRate(new Runnable() {
            @Override
            public void run() {
                flushAllLogs();
            }
        }, config.flushSchedulerThreadRate, config.flushSchedulerThreadRate);
    }

    private void awaitStartup() throws InterruptedException{
        if (config.enableZookeeper)
            startupLatch.await();
    }

    private void registerNewTopicInZK(String topic) {
        if (config.enableZookeeper)
            kafkaZookeeper.registerTopicInZk(topic);
    }

    /**
     * Create a log for the given topic and the given partition
     */
    private Log createLog(String topic, int partition) throws IOException{
        synchronized(logCreationLock) {
            File d = new File(logDir, topic + "-" + partition);
            d.mkdirs();
            Long rollIntervalMs = logRollMsMap.getOrDefault(topic,this.logRollDefaultIntervalMs);
            Integer maxLogFileSize = logFileSizeMap.getOrDefault(topic,config.logFileSize);
            return new Log(d, milliseconds, maxLogFileSize, config.maxMessageSize, flushInterval, rollIntervalMs, false);
        }
    }

    /**
     * Return the Pool (partitions) for a specific log
     */
    private Pool<Integer,Log> getLogPool(String topic, int partition) throws InterruptedException{
        awaitStartup();
        topicNameValidator.validate(topic);
        if (partition < 0 || partition >= topicPartitionsMap.getOrDefault(topic, numPartitions)) {
            logger.warn("Wrong partition " + partition + " valid partitions (0," +
                    (topicPartitionsMap.getOrDefault(topic, numPartitions) - 1) + ")");
            throw new InvalidPartitionException("wrong partition " + partition);
        }
        return logs.get(topic);
    }

    /**
     * Pick a random partition from the given topic
     */
    public int chooseRandomPartition(String topic) {
        return random.nextInt(topicPartitionsMap.getOrDefault(topic, numPartitions));
    }

    public long[] getOffsets(OffsetRequest offsetRequest)throws InterruptedException {
        Log log = getLog(offsetRequest.topic, offsetRequest.partition);
        if (log != null) return log.getOffsetsBefore(offsetRequest);
        return Log.getEmptyOffsets(offsetRequest);
    }

    /**
     * Get the log if exists
     */
    public Log getLog(String topic, int partition) throws InterruptedException{
        Pool<Integer,Log> parts = getLogPool(topic, partition);
        if (parts == null) return null;
        return parts.get(partition);
    }

    /**
     * Create the log if it does not exist, if it exists just return it
     */
    public Log getOrCreateLog(String topic, int partition)throws InterruptedException,IOException {
        boolean hasNewTopic = false;
        Pool<Integer,Log> parts = getLogPool(topic, partition);
        if (parts == null) {
            Pool<Integer,Log> found = logs.putIfNotExists(topic, new Pool<>());
            if (found == null)
                hasNewTopic = true;
            parts = logs.get(topic);
        }
        Log log = parts.get(partition);
        if(log == null) {
            log = createLog(topic, partition);
            Log found = parts.putIfNotExists(partition, log);
            if(found != null) {
                // there was already somebody there
                log.close();
                log = found;
            }
            else
                logger.info("Created log for '" + topic + "'-" + partition);
        }

        if (hasNewTopic)
            registerNewTopicInZK(topic);
        return log;
    }

    /* Attemps to delete all provided segments from a log and returns how many it was able to */
    private int deleteSegments(Log log, Log.LogSegment...segments) throws IOException{
        int total = 0;
        for(Log.LogSegment segment : segments) {
            logger.info("Deleting log segment " + segment.file.getName() + " from " + log.name);
            segment.messageSet.close();
            if(!segment.file.delete()) {
                logger.warn("Delete failed.");
            } else {
                total += 1;
            }
        }
        return total;
    }

    /* Runs through the log removing segments older than a certain age */
    private int cleanupExpiredSegments(Log log) throws IOException{
        long startMs = milliseconds;
        String topic = Utils.getTopicPartition(log.dir().getName()).getKey();
        long logCleanupThresholdMS = logRetentionMsMap.getOrDefault(topic,this.logCleanupDefaultAgeMs);
        List<Log.LogSegment> logSegments = log.segments().view();
        List<Log.LogSegment> list = new ArrayList<>();
        for(Log.LogSegment segment : logSegments){
            if(startMs - segment.file.lastModified() > logCleanupThresholdMS){
                list.add(segment);
            }
        }
        Log.LogSegment[] toBeDeleted = new Log.LogSegment[list.size()];
        for(int i = 0;i < list.size();i++){
            toBeDeleted[i] = list.get(i);
        }
        int total = deleteSegments(log, toBeDeleted);
        return total;
    }

    /**
     *  Runs through the log removing segments until the size of the log
     *  is at least logRetentionSize bytes in size
     */
    private int cleanupSegmentsToMaintainSize(Log log) throws IOException{
        String topic = Utils.getTopicPartition(log.dir().getName()).getKey();
        int maxLogRetentionSize = logRetentionSizeMap.getOrDefault(topic,(int)config.logRetentionSize);
        if(maxLogRetentionSize < 0 || log.size() < maxLogRetentionSize) return 0;
        long diff = log.size() - maxLogRetentionSize;
        List<Log.LogSegment> logSegments = log.segments().view();
        List<Log.LogSegment> list = new ArrayList<>();
        for(Log.LogSegment segment : logSegments){
            if(diff - segment.size() >= 0) {
                list.add(segment);
                diff -= segment.size();
            }
        }
        Log.LogSegment[] shouldDelete = new Log.LogSegment[list.size()];
        for(int i = 0;i < list.size();i++){
            shouldDelete[i] = list.get(i);
        }
        Log.LogSegment[] toBeDeleted = log.markDeletedWhile(shouldDelete);
        int total = deleteSegments(log, toBeDeleted);
        return total;
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     */
    public void cleanupLogs() throws IOException{
        logger.debug("Beginning log cleanup...");
        Iterator<Log> iter = getLogIterator();
        int total = 0;
        long startMs = milliseconds;
        while(iter.hasNext()) {
            Log log = iter.next();
            logger.debug("Garbage collecting '" + log.name + "'");
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        logger.debug("Log cleanup completed. " + total + " files deleted in " +
                (milliseconds - startMs) / 1000 + " seconds");
    }

    /**
     * Close all the logs
     */
    public void close() throws IOException{
        logFlusherScheduler.shutdown();
        Iterator<Log> iter = getLogIterator();
        while(iter.hasNext())
            iter.next().close();
        if (config.enableZookeeper) {
            kafkaZookeeper.close();
        }
    }

    private Iterator<Log> getLogIterator(){
        return new IteratorTemplate<Log>() {
            Iterator<Pool<Integer,Log>> partsIter = logs.values().iterator();
            Iterator<Log> logIter = null;

            public Log makeNext() {
                while (true) {
                    if (logIter != null && logIter.hasNext())
                        return logIter.next();
                    if (!partsIter.hasNext())
                        return allDone();
                    logIter = partsIter.next().values().iterator();
                }
            }
        };
    }

    private void flushAllLogs()  {
        logger.debug("flushing the high watermark of all logs");
        Iterator<Log> iterator = getLogIterator();
        while (iterator.hasNext()){
            Log log = iterator.next();

            try{
                long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                long logFlushInterval = config.defaultFlushIntervalMs;
                if(logFlushIntervalMap.containsKey(log.getTopicName()))
                    logFlushInterval = logFlushIntervalMap.get(log.getTopicName());
                logger.debug(log.getTopicName() + " flush interval  " + logFlushInterval +
                        " last flushed " + log.getLastFlushedTime() + " timesincelastFlush: " + timeSinceLastFlush);
                if(timeSinceLastFlush >= logFlushInterval)
                    log.flush();
            }
            catch (IOException e){
                logger.error("Error flushing topic " + log.getTopicName(), e);
                Runtime.getRuntime().halt(1);
            }
        }
    }


    public Iterator<String> getAllTopics(){
        return logs.keys().iterator();
    }
    public Map<String,Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }

}

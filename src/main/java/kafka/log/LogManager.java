package kafka.log;


import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.server.KafkaConfig;
import kafka.utils.FileLock;
import kafka.utils.KafkaScheduler;
import kafka.utils.Pool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
public class LogManager {

    private static Logger logger = Logger.getLogger(LogManager.class);

    public static final String CleanShutdownFile = ".kafka_cleanshutdown";
    public static final String LockFile = ".lock";

    KafkaConfig config;
    KafkaScheduler scheduler;
    long milliseconds;

    public LogManager(KafkaConfig config, KafkaScheduler scheduler, long milliseconds) throws IOException {
        this.config = config;
        this.scheduler = scheduler;
        this.milliseconds = milliseconds;

        logDirs = new File[config.logDirs.size()];
        for(int i = 0;i < config.logDirs.size();i++){
            logDirs[i] = new File(config.logDirs.get(i));
        }
        logFileSizeMap = config.logSegmentBytesPerTopicMap;
        logFlushInterval = config.logFlushIntervalMessages;
        logFlushIntervals = config.logFlushIntervalMsPerTopicMap;
        logRetentionSizeMap = config.logRetentionBytesPerTopicMap;
        logRollDefaultIntervalMs = 1000L * 60 * 60 * config.logRollHours;
        logCleanupIntervalMs = 1000L * 60 * config.logCleanupIntervalMins;
        logCleanupDefaultAgeMs = 1000L * 60 * 60 * config.logRetentionHours;

        config.logRetentionHoursPerTopicMap.forEach((k, v) -> logRetentionMsMap.put(k,Long.parseLong(v) * 60 * 60 * 1000L));
        config.logRollHoursPerTopicMap.forEach((k, v) -> logRollMsMap.put(k,Long.parseLong(v) * 60 * 60 * 1000L));

        createAndValidateLogDirs(logDirs);
        dirLocks = lockLogDirs(logDirs);
        loadLogs(logDirs);
    }

    File[] logDirs ;
    private Map<String,String> logFileSizeMap;
    private int logFlushInterval;
    private Map<String,String> logFlushIntervals ;
    private Object logCreationLock = new Object();
    private Map<String,String> logRetentionSizeMap;
    private Map<String,Long> logRetentionMsMap ; // convert hours to ms
    private Map<String,Long> logRollMsMap ;
    private long logRollDefaultIntervalMs ;
    private long logCleanupIntervalMs;
    private long logCleanupDefaultAgeMs;

    private Pool<TopicAndPartition, Log> logs = new Pool<TopicAndPartition, Log>();
    List<FileLock> dirLocks;


    /**
     * 1. Ensure that there are no duplicates in the directory list
     * 2. Create each directory if it doesn't exist
     * 3. Check that each path is a readable directory
     */
    private void createAndValidateLogDirs(File[] dirs) throws IOException {
        Set<String> set = new HashSet<>();
        for(File file:dirs){
            set.add(file.getCanonicalPath());
        }
        if(set.size() < dirs.length)
            throw new KafkaException("Duplicate log directory found: " + logDirs);
        for(File dir : dirs) {
            if(!dir.exists()) {
                logger.info("Log directory '" + dir.getAbsolutePath() + "' not found, creating it.");
                boolean created = dir.mkdirs();
                if(!created)
                    throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath());
            }
            if(!dir.isDirectory() || !dir.canRead())
                throw new KafkaException(dir.getAbsolutePath() + " is not a readable log directory.");
        }
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(File[] dirs) throws IOException{
        List<FileLock> fileLocks = new ArrayList<>();
        for(File dir : dirs) {
            FileLock lock = new FileLock(new File(dir, LockFile));
            if(!lock.tryLock())
                throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile().getAbsolutePath() +
                        ". A Kafka instance in another process or thread is using this directory.");
            fileLocks.add(lock);
        }
        return fileLocks;
    }

    /**
     * Recovery and load all logs in the given data directories
     */
    private void loadLogs(File[] dirs) throws IOException {
        for(File dir : dirs) {
            /* check if this set of logs was shut down cleanly */
            File cleanShutDownFile = new File(dir, CleanShutdownFile);
            boolean needsRecovery = !cleanShutDownFile.exists();
            cleanShutDownFile.delete();
            /* load the logs */
            File[] subDirs = dir.listFiles();
            if(subDirs != null) {
                for(File d : subDirs) {
                    if(d.isDirectory()){
                        logger.info("Loading log '" + d.getName() + "'");
                        TopicAndPartition topicPartition = parseTopicPartitionName(d.getName());
                        long rollIntervalMs = logRollMsMap.getOrDefault(topicPartition.topic(),this.logRollDefaultIntervalMs);
                        long maxLogFileSize = Long.parseLong(logFileSizeMap.getOrDefault(topicPartition.topic(),String.valueOf(config.logSegmentBytes)));
                        Log log = new Log(d,
                                maxLogFileSize,
                                config.messageMaxBytes,
                                logFlushInterval,
                                rollIntervalMs,
                                needsRecovery,
                                milliseconds,
                                config.logIndexSizeMaxBytes,
                                config.logIndexIntervalBytes,
                                config.brokerId);
                        Log previous = this.logs.put(topicPartition, log);
                        if(previous != null)
                            throw new IllegalArgumentException("Duplicate log directories found: %s, %s!".format(log.dir().getAbsolutePath(), previous.dir().getAbsolutePath()));
                    }
                }
            }
        }
    }

    /**
     *  Start the log flush thread
     */
    public void startup() {
        /* Schedule the cleanup task to delete old logs */
        if(scheduler != null) {
            logger.info("Starting log cleaner every " + logCleanupIntervalMs + " ms");
            scheduler.scheduleWithRate(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(scheduler.currentThreadName("kafka-logcleaner-"));
                    try {
                        cleanupLogs();
                    }catch (IOException e){
                        logger.error("cleanupLogs error:",e);
                    }
                }
            },  60 * 1000, logCleanupIntervalMs, false);
            logger.info("Starting log flusher every " + config.logFlushSchedulerIntervalMs +
                    " ms with the following overrides " + logFlushIntervals);
            scheduler.scheduleWithRate(new Runnable() {
                                           @Override
                                           public void run() {
                                               Thread.currentThread().setName(scheduler.currentThreadName( "kafka-logflusher-"));
                                               flushDirtyLogs();
                                           }
                                       },
                    config.logFlushSchedulerIntervalMs, config.logFlushSchedulerIntervalMs, false);
        }
    }

    /**
     * Get the log if it exists
     */
    public Log getLog(String topic, int partition) {
        TopicAndPartition topicAndPartiton = new TopicAndPartition(topic, partition);
        Log log = logs.get(topicAndPartiton);
        if (log == null)
            return null;
        return log;
    }

    /**
     * Create the log if it does not exist, if it exists just return it
     */
    public Log getOrCreateLog(String topic, int partition) throws IOException{
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Log log = logs.get(topicAndPartition) ;
        if(log == null) createLogIfNotExists(topicAndPartition);
        return log;
    }

    /**
     * Create a log for the given topic and the given partition
     * If the log already exists, just return a copy of the existing log
     */
    private Log createLogIfNotExists(TopicAndPartition topicAndPartition) throws IOException{
        synchronized(logCreationLock) {
            Log log = logs.get(topicAndPartition);

            // check if the log has already been created in another thread
            if(log != null)
                return log;

            // if not, create it
            File dataDir = nextLogDir();
            File dir = new File(dataDir, topicAndPartition.topic() + "-" + topicAndPartition.partition());
            dir.mkdirs();
            long rollIntervalMs = logRollMsMap.getOrDefault(topicAndPartition.topic(),this.logRollDefaultIntervalMs);
            long maxLogFileSize = Long.parseLong(logFileSizeMap.getOrDefault(topicAndPartition.topic(),String.valueOf(config.logSegmentBytes)));
            log = new Log(dir,
                    maxLogFileSize,
                    config.messageMaxBytes,
                    logFlushInterval,
                    rollIntervalMs,
                   false,
                    milliseconds,
                    config.logIndexSizeMaxBytes,
                    config.logIndexIntervalBytes,
                    config.brokerId);
            logger.info("Created log for partition [%s,%d] in %s.".format(topicAndPartition.topic(), topicAndPartition.partition(), dataDir.getAbsolutePath()));
            logs.put(topicAndPartition, log);
            return log;
        }
    }

    /**
     * Choose the next directory in which to create a log. Currently this is done
     * by calculating the number of partitions in each directory and then choosing the
     * data directory with the fewest partitions.
     */
    private File nextLogDir() {
        if(logDirs.length == 1) {
            return logDirs[0];
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories
            Iterable<Log> allLogs = allLogs();
            Map<String,List<Log>> logDirMap = new HashMap<>();
            for(Log log:allLogs){
                String p = log.dir().getParent();
                List<Log> list = logDirMap.get(p);
                if(list == null){
                    list = new ArrayList<>();
                    logDirMap.put(p,list);
                }
                list.add(log);
            }
            Map<String,Integer> logCounts = new HashMap<>();
            for(Map.Entry<String,List<Log>> entry : logDirMap.entrySet()){
                logCounts.put(entry.getKey(),entry.getValue().size());
            }
            Map<String,Integer> zeros = new HashMap<>();
            for(File file:logDirs){
                zeros.put(file.getPath(),0);
            }
            zeros.putAll(logCounts);
            LinkedHashMap<String,Integer>  linkedHashMap = zeros.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
            Set<Map.Entry<String,Integer>> set = linkedHashMap.entrySet();
            String leastLoaded = null;
            for(Map.Entry<String,Integer> entry:set){
                leastLoaded = entry.getKey();
                break;
            }
            // choose the directory with the least logs in it
            return new File(leastLoaded);
        }
    }

    public Long[] getOffsets(TopicAndPartition topicAndPartition, long timestamp, int maxNumOffsets) {
        Log log = getLog(topicAndPartition.topic(), topicAndPartition.partition());
        if(log == null){
            long[] offsets = log.getOffsetsBefore(timestamp, maxNumOffsets);
            Long[] l = new  Long[offsets.length];
            for(int i = 0;i < offsets.length;i++){
                l[i] = offsets[i];
            }
            return l;
        }else{
            Long r =  Log.getEmptyOffsets(timestamp);
            if(r == null) return null;
            Long[] l = new  Long[1];
            l[0] = r;
            return l;
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     */
    private int cleanupExpiredSegments(Log log) throws IOException {
        long startMs = System.currentTimeMillis();
        String topic = parseTopicPartitionName(log.name).topic();
        long logCleanupThresholdMs = logRetentionMsMap.getOrDefault(topic,this.logCleanupDefaultAgeMs);
        List<LogSegment> list = new ArrayList<>();
        SegmentList<LogSegment> segment = log.segments();
        for(LogSegment logSegment : segment.view()){
            if(startMs - logSegment.messageSet.file.lastModified() > logCleanupThresholdMs){
                list.add(logSegment);
            }
        }
        LogSegment[] deletable = new LogSegment[list.size()];
        LogSegment[] toBeDeleted =  log.markDeletedWhile(list.toArray(deletable));
        int total = log.deleteSegments(list);
        return total;
    }

    /**
     *  Runs through the log removing segments until the size of the log
     *  is at least logRetentionSize bytes in size
     */
    private int cleanupSegmentsToMaintainSize(Log log) throws IOException {
        String topic = parseTopicPartitionName(log.dir().getName()).topic();
        int maxLogRetentionSize = Integer.parseInt(logRetentionSizeMap.getOrDefault(topic,String.valueOf(config.logRetentionBytes)));
        if(maxLogRetentionSize < 0 || log.size() < maxLogRetentionSize) return 0;
        long diff = log.size() - maxLogRetentionSize;
        SegmentList<LogSegment> segment = log.segments();
        List<LogSegment> list = new ArrayList<>();
        for(LogSegment logSegment : segment.view()){
            if(diff - logSegment.size() >= 0) {
                diff -= logSegment.size();
                list.add(logSegment);
            }
        }
        LogSegment[] deletable = new LogSegment[list.size()];
        LogSegment[] toBeDeleted = log.markDeletedWhile(list.toArray(deletable));
        int total = log.deleteSegments(list);
        return total;
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     */
    public void cleanupLogs() throws IOException{
       logger.debug("Beginning log cleanup...");
        int total = 0;
        long startMs = System.currentTimeMillis();
        for(Log log : allLogs()) {
            logger.debug("Garbage collecting '" + log.name + "'");
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        logger.debug("Log cleanup completed. " + total + " files deleted in " +
                (System.currentTimeMillis() - startMs) / 1000 + " seconds");
    }

    /**
     * Close all the logs
     */
    public void shutdown() throws IOException{
        logger.debug("Shutting down.");
        try {
            // close the logs
            for(Log log : allLogs()) {
                log.close();
            }
            // mark that the shutdown was clean by creating the clean shutdown marker file
            for(File file:logDirs){
                new File(file, CleanShutdownFile).createNewFile();
            }
        } finally {
            // regardless of whether the close succeeded, we need to unlock the data directories
            for(FileLock fileLock:dirLocks){
                fileLock.destroy();
            }
        }
        logger.debug("Shutdown complete.");
    }

    /**
     * Get all the partition logs
     */
    public Iterable<Log> allLogs(){
        return logs.values();
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private void flushDirtyLogs() {
       logger.debug("Checking for dirty logs to flush...");
        for (Log log : allLogs()) {
            try {
                long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                long logFlushInterval = config.logFlushIntervalMs;
                if(logFlushIntervals.containsKey(log.topicName()))
                    logFlushInterval = Long.parseLong(logFlushIntervals.get(log.topicName()));
                logger.debug(log.topicName() + " flush interval  " + logFlushInterval +
                        " last flushed " + log.getLastFlushedTime() + " time since last flush: " + timeSinceLastFlush);
                if(timeSinceLastFlush >= logFlushInterval)
                    log.flush();
            } catch (IOException e1){
                logger.fatal("Halting due to unrecoverable I/O error while flushing logs: " + e1.getMessage(), e1);
                System.exit(1);
            }catch (Throwable e){
                logger.error("Error flushing topic " + log.topicName(), e);
            }
        }
    }

    private TopicAndPartition parseTopicPartitionName(String name) {
        int index = name.lastIndexOf('-');
        return new TopicAndPartition(name.substring(0,index), Integer.parseInt(name.substring(index+1)));
    }

    public Iterable<String> topics()  {
        return logs.keys().stream().map(t->t.topic()).collect(Collectors.toList());
    }

}

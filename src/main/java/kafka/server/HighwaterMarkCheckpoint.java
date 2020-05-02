package kafka.server;

import kafka.common.TopicAndPartition;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class HighwaterMarkCheckpoint {

    private static Logger logger = Logger.getLogger(HighwaterMarkCheckpoint.class);

    public static String highWatermarkFileName = "replication-offset-checkpoint";
    public static int currentHighwaterMarkFileVersion = 0;

    String path;

    public HighwaterMarkCheckpoint(String path) {
        this.path = path;

        name = path + File.separator + HighwaterMarkCheckpoint.highWatermarkFileName;
        hwFile = new File(name);
    }

    /* create the highwatermark file handle for all partitions */
    String name;
    private File hwFile ;
    private ReentrantLock hwFileLock = new ReentrantLock();
    // recover from previous tmp file, if required

    public void write(Map<TopicAndPartition, Long> highwaterMarksPerPartition) throws IOException {
        hwFileLock.lock();
        try {
            // write to temp file and then swap with the highwatermark file
            File tempHwFile = new File(hwFile + ".tmp");

            BufferedWriter hwFileWriter = new BufferedWriter(new FileWriter(tempHwFile));
            // checkpoint highwatermark for all partitions
            // write the current version
            hwFileWriter.write(HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion+"");
            hwFileWriter.newLine();
            // write the number of entries in the highwatermark file
            hwFileWriter.write(highwaterMarksPerPartition.size()+"");
            hwFileWriter.newLine();
            for(Map.Entry<TopicAndPartition, Long> entry : highwaterMarksPerPartition.entrySet()){
                hwFileWriter.write(String.format("%s %s %s",entry.getKey().topic(), entry.getKey().partition(), entry.getValue()));
                hwFileWriter.newLine();
            }
            hwFileWriter.flush();
            hwFileWriter.close();
            // swap new high watermark file with previous one
            if(!tempHwFile.renameTo(hwFile)) {
                // renameTo() fails on Windows if the destination file exists.
                hwFile.delete();
                if(!tempHwFile.renameTo(hwFile)) {
                   logger.fatal("Attempt to swap the new high watermark file with the old one failed");
                    System.exit(1);
                }
            }
        }finally {
            hwFileLock.unlock();
        }
    }

    public long read(String topic, int partition) throws IOException {
        hwFileLock.lock();
        try {
            if(hwFile.length() == 0){
                logger.warn(String.format("No highwatermark file is found. Returning 0 as the highwatermark for partition [%s,%d]",topic, partition));
                return 0L;
            }else{
                BufferedReader hwFileReader = new BufferedReader(new FileReader(hwFile));
                short version = Short.parseShort(hwFileReader.readLine());
                if(version == HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion){
                    int numberOfHighWatermarks = Integer.parseInt(hwFileReader.readLine());
                    Map<TopicAndPartition,Long> partitionHighWatermarks = new HashMap<>();

                    for(int i = 0 ;i < numberOfHighWatermarks;i++) {
                        String nextHwEntry = hwFileReader.readLine();
                        String[] partitionHwInfo = nextHwEntry.split(" ");
                        String temTopic = partitionHwInfo[0];
                        Integer partitionId = Integer.parseInt(partitionHwInfo[1]);
                        Long highWatermark = Long.parseLong(partitionHwInfo[2]);
                        partitionHighWatermarks.put(new TopicAndPartition(temTopic, partitionId) , highWatermark);
                    }
                    hwFileReader.close();
                    Long hwOpt = partitionHighWatermarks.get(new TopicAndPartition(topic, partition));
                    if(hwOpt == null){
                        logger.warn(String.format(String.format("No previously checkpointed highwatermark value found for topic %s ",topic) +
                                "partition %d. Returning 0 as the highwatermark",partition));
                       return  0L;
                    }else{
                        logger.debug(String.format("Read hw %d for partition [%s,%d] from highwatermark checkpoint file",hwOpt, topic, partition));
                        return hwOpt;
                    }

                }else{
                    logger.fatal("Unrecognized version of the highwatermark checkpoint file " + version);
                    System.exit(1);
                    return  -1L;
                }
            }
        }finally {
            hwFileLock.unlock();
        }
    }
}

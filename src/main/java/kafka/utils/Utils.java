package kafka.utils;

import kafka.cluster.Partition;
import kafka.message.CompressionCodec;
import kafka.message.CompressionFactory;
import kafka.message.NoCompressionCodec;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.zip.CRC32;

import static kafka.utils.Preconditions.checkArgument;

public class Utils {

    private static Logger logger = Logger.getLogger(Utils.class);
    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void putUnsignedInt(ByteBuffer buffer, long value){
        buffer.putInt((int)(value & 0xffffffffL));
    }


    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void putUnsignedInt(ByteBuffer buffer, int index, long value){
        buffer.putInt(index, (int)(value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static long crc32(byte[]  bytes){
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(byte[] bytes, int offset,int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long getUnsignedInt(ByteBuffer buffer){
        return buffer.getInt() & 0xffffffffL;
    }
    /**
     * Read an unsigned integer from the given position without modifying the buffers
     * position
     * @param buffer The buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long  getUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }


    /**
     * Open a channel for the given file
     */
    public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException {
        if(mutable)
            return new RandomAccessFile(file, "rw").getChannel();
        else
            return new FileInputStream(file).getChannel();
    }

    /**
     * Read size prefixed string where the size is stored as a 2 byte short.
     * @param buffer The buffer to read from
     * @param encoding The encoding in which to read the string
     */
    public static String readShortString(ByteBuffer buffer, String encoding)  throws UnsupportedEncodingException {
        int size = buffer.getShort();
        if(size < 0)
            return null;
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes, encoding);
    }
    /**
     * Write a size prefixed string where the size is stored as a 2 byte short
     * @param buffer The buffer to write to
     * @param string The string to write
     * @param encoding The encoding in which to write the string
     */
    public static void writeShortString(ByteBuffer buffer, String string, String encoding) throws UnsupportedEncodingException{
        if(string == null) {
            buffer.putShort((short)-1);
        } else if(string.length() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
        } else {
            buffer.putShort((short) string.length());
            buffer.put(string.getBytes(encoding));
        }
    }

    /**
     * Compute the hash code for the given items
     */
    public static int hashcode(Object...obj) {
        if(obj == null)
            return 0;
        int h = 1;
        int i = 0;
        while(i < obj.length) {
            if(obj[i] != null) {
                h = 31 * h + obj[i].hashCode();
                i += 1;
            }
        }
        return h;
    }

    public static int  getInt(Properties props,String name) {
        if(props.containsKey(name))
            return getInt(props, name, -1);
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
    public static int getInt(Properties props,String name, int defaultValue){
        if(props.containsKey(name))
            return Integer.parseInt(props.get(name).toString());
        return defaultValue;
    }
    public static int getIntInRange(Properties props,String name, int defaultValue, int start,int end){
        int v = -1;
        if(props.containsKey(name))
            v = Integer.parseInt(props.get(name).toString());
        else
            v = defaultValue;

        if(v < start || v > end)
            throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start +","+ end + ".");
        return v;
    }


    public static long getLong(Properties props,String name) {
        if(props.containsKey(name))
            return getLong(props, name, -1);
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }


    public static long getLong(Properties props,String name, long defaultValue){
        return  getLongInRange(props, name, defaultValue, Long.MIN_VALUE, Long.MAX_VALUE);
    }



    public static long getLongInRange(Properties props,String name, long defaultValue, long start,long end) {
        long v = -1;
        if(props.containsKey(name))
            v = Long.parseLong(props.get(name).toString());
        else
            v = defaultValue;

        if(v < start || v > end)
            throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start +","+ end + ".");
        return v;
    }
    /**
     * Get a string property, or, if no such property is defined, return the given default value
     */
    public static String getString(Properties props,String name, String defaultValue) {
        if(props.containsKey(name))
            return props.get(name).toString();
        return defaultValue;
    }

    /**
     * Get a string property or throw and exception if no such property is defined.
     */
    public static String getString(Properties props,String name) {
        if(props.containsKey(name))
            return  props.get(name).toString();
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    /**
     * Read a boolean value from the properties instance
     * @param props The properties to read from
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @return the boolean value
     */
    public static boolean getBoolean(Properties props,String name,boolean defaultValue) {
        if(!props.containsKey(name))
            return defaultValue;
        else if("true".equals(props.get(name) == null?null:props.get(name).toString()))
            return true;
        else if("false".equals(props.get(name) == null?null:props.get(name).toString()))
            return false;
        else
            throw new IllegalArgumentException("Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false" );
    }

    public static Map<String,Integer> getTopicFileSize(String fileSizes){
        String exceptionMsg = "Malformed token for topic.log.file.size in server.properties: ";
        String successMsg =  "The log file size for ";
        Map<String,Integer> map = getCSVMap(fileSizes, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer size = entry.getValue();
            checkArgument(size > 0, "Log file size value for topic " + topic + " is " + size +
                    " which is not greater than 0.");
        }
        return map;
    }

    public static Map<String,Integer> getTopicRollHours(String rollHours)  {
        String exceptionMsg = "Malformed token for topic.log.roll.hours in server.properties: ";
        String successMsg =  "The roll hours for ";
        Map<String,Integer> map = getCSVMap(rollHours, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer hrs = entry.getValue();
            checkArgument(hrs > 0, "Log roll hours value for topic " + topic + " is " + hrs +
                    " which is not greater than 0.");
        }
        return map;
    }

    public static Map<String,Integer> getTopicRetentionHours(String retentionHours)  {
        String exceptionMsg = "Malformed token for topic.log.retention.hours in server.properties: ";
        String successMsg =  "The retention hours for ";
        Map<String,Integer> map = getCSVMap(retentionHours, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer hrs = entry.getValue();
            checkArgument(hrs > 0, "Log retention hours value for topic " + topic + " is " + hrs +
                    " which is not greater than 0.");
        }
        return map;
    }
    public static Map<String,Integer> getTopicRetentionSize(String retentionSizes)  {
        String exceptionMsg = "Malformed token for topic.log.retention.size in server.properties: ";
        String successMsg =  "The log retention size for ";
        Map<String,Integer> map = getCSVMap(retentionSizes, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer size = entry.getValue();
            checkArgument(size > 0, "Log retention size value for topic " + topic + " is " + size +
                    " which is not greater than 0.");
        }
        return map;
    }

    public static Map<String,Integer> getTopicFlushIntervals(String allIntervals)  {
        String exceptionMsg = "Malformed token for topic.flush.Intervals.ms in server.properties: ";
        String successMsg =  "The flush interval for ";
        Map<String,Integer> map = getCSVMap(allIntervals, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer interval = entry.getValue();
            checkArgument(interval > 0, "Flush interval value for topic " + topic + " is " + interval +
                    " ms which is not greater than 0.");
        }
        return map;
    }


    public static Map<String,Integer> getTopicPartitions(String allPartitions)  {
        String exceptionMsg = "Malformed token for topic.partition.counts in server.properties: ";
        String successMsg =  "The number of partitions for topic  ";
        Map<String,Integer> map = getCSVMap(allPartitions, exceptionMsg, successMsg);
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            String topic = entry.getKey();
            Integer count = entry.getValue();
            checkArgument(count > 0, "The number of partitions for topic " + topic + " is " + count +
                    " which is not greater than 0.");
        }
        return map;
    }


    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
     */
    private  static Map<String,Integer> getCSVMap(String allCSVals, String exceptionMsg, String successMsg)  {
        Map<String,Integer> map = new HashMap<>();
        if("".equals(allCSVals))
            return map;
        String[] csVals = allCSVals.split(",");
        for(int i = 0 ;i < csVals.length; i++) {
            String[] tempSplit = csVals[i].split(":");
            logger.info(successMsg + tempSplit[0] + " : " + Integer.parseInt(tempSplit[1].trim()));
            map.put(tempSplit[0],Integer.parseInt(tempSplit[1].trim()));
        }
        return map;
    }

    public static Pair<String, Integer> getTopicPartition(String topicPartition)  {
        int index = topicPartition.lastIndexOf('-');
        return new Pair<String, Integer> (topicPartition.substring(0,index), Integer.parseInt(topicPartition.substring(index+1)));
    }

    public static  Thread newThread(String name, Runnable runnable, boolean daemon){
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
    }

    public static boolean propertyExists(String prop) {
        if(prop == null)
            return false;
        else if(prop.compareTo("") == 0)
            return false;
        else return true;
    }

    public static CompressionCodec getCompressionCodec(Properties props, String codec) {
        Object codecValueString = props.get(codec);
        if(codecValueString == null)
           return new NoCompressionCodec();
        else
            return CompressionFactory.getCompressionCodec(Integer.parseInt(codecValueString.toString()));
    }

    public static List<String> getCSVList(String csvList) {
        List<String> list = new ArrayList<>();
        if(csvList != null){
            String[] arr = csvList.split(",");
            for(String str:arr){
                if(!str.equals("")){
                    list.add(str);
                }
            }
        }
        return  list;
    }

    /**
     * Get a property of type java.util.Properties or throw and exception if no such property is defined.
     */
    public static  Properties getProps(Properties props, String name) {
        if(props.containsKey(name)) {
            String propString = props.get(name).toString();
            String[] propValues = propString.split(",");
            Properties properties = new Properties();
            for(int i = 0;i < propValues.length;i++) {
                String[] prop = propValues[i].split("=");
                if(prop.length != 2)
                    throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
                properties.put(prop[0], prop[1]);
            }
           return properties;
        }
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    /**
     * Get a property of type java.util.Properties or return the default if no such property is defined
     */
    public static  Properties getProps(Properties props, String name, Properties defaultProperties){
        if(props.containsKey(name)) {
            String propString = props.get(name).toString();
            String[] propValues = propString.split(",");
            if(propValues.length < 1)
                throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
            Properties properties = new Properties();
            for(int i = 0;i < propValues.length;i++) {
                String[] prop = propValues[i].split("=");
                if(prop.length != 2)
                    throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
                properties.put(prop[0], prop[1]);
            }
            return  properties;
        }
        else
            return defaultProperties;
    }

    public static Object getObject(String className)  throws ClassNotFoundException,InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        if(className == null || className.isEmpty()){
            return null;
        }
        Class<?> clazz = Class.forName(className);
        Constructor<?>[] constructors = clazz.getConstructors();
        checkArgument(constructors.length == 1);
        return constructors[0].newInstance();
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     */
    public static Properties loadProps(String filename) throws IOException{
        FileInputStream propStream = new FileInputStream(filename);
        Properties props = new Properties();
        props.load(propStream);
        return props;
    }

    public static TreeSet<Partition> getTreeSetSet(){
        TreeSet<Partition> treeSet = new TreeSet<>(new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                if (o1.brokerId() == o2.brokerId())
                    return o1.partId() - o2.partId();
                else
                    return o1.brokerId() - o2.brokerId();
            }
        });
        return treeSet;
    }
}

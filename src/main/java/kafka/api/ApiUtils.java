package kafka.api;

import kafka.common.KafkaException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class ApiUtils {

    public static String ProtocolEncoding = "UTF-8";

    /**
     * Read size prefixed string where the size is stored as a 2 byte short.
     * @param buffer The buffer to read from
     */
     public static String readShortString(ByteBuffer buffer) throws UnsupportedEncodingException {
        int size = buffer.getShort();
        if(size < 0)
            return null;
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes, ProtocolEncoding);
    }

    /**
     * Write a size prefixed string where the size is stored as a 2 byte short
     * @param buffer The buffer to write to
     * @param string The string to write
     */
     public static void writeShortString(ByteBuffer buffer,String string) throws UnsupportedEncodingException{
        if(string == null) {
            buffer.putShort((short)-1);
        } else {
            byte[]  encodedString = string.getBytes(ProtocolEncoding);
            if(encodedString.length > Short.MAX_VALUE) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
            } else {
                buffer.putShort((short) encodedString.length);
                buffer.put(encodedString);
            }
        }
    }

    /**
     * Return size of a size prefixed string where the size is stored as a 2 byte short
     * @param string The string to write
     */
     public static int shortStringLength(String string) throws UnsupportedEncodingException{
        if(string == null) {
            return 2;
        } else {
            byte[] encodedString = string.getBytes(ProtocolEncoding);
            if(encodedString.length > Short.MAX_VALUE) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
            } else {
               return  2 + encodedString.length;
            }
        }
    }

    /**
     * Read an integer out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
     public static int readIntInRange(ByteBuffer buffer,String name, int s,int e){
        int value = buffer.getInt();
        if(value < s || value > e)
            throw new KafkaException(name + " has value " + value + " which is not in the range [" + s + "," + e + "].");
        else return value;
    }

    /**
     * Read a short out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
     public static short readShortInRange(ByteBuffer buffer,String name, short s,short e) {
         short value = buffer.getShort();
        if(value < s || value > e)
            throw new KafkaException(name + " has value " + value + " which is not in the range "+ s + "," + e + ".");
        else return value;
    }

    /**
     * Read a long out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
     public static long readLongInRange(ByteBuffer buffer,String name, long s,long e) {
        long value = buffer.getLong();
        if(value < s || value > e)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + s + "," + e + ".");
        else return value;
    }

}

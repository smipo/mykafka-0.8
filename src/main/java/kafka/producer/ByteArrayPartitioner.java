package kafka.producer;

import kafka.utils.Utils;

public class ByteArrayPartitioner implements Partitioner<Byte[]> {

    public int partition(Byte[] key, int numPartitions){
        return  Utils.abs(java.util.Arrays.hashCode(key)) % numPartitions;
    }
}

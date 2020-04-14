package kafka.producer;

import kafka.utils.Utils;

import java.util.Random;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class DefaultPartitioner<T> implements Partitioner<T> {


    public int partition(T key, int numPartitions) {
        return  Utils.abs(key.hashCode()) % numPartitions;
    }
}

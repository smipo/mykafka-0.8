package kafka.producer;

import java.util.Random;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class DefaultPartitioner<T> implements Partitioner<T> {

    private Random random = new Random();

    public int partition(T key, int numPartitions) {
        if(key == null)
            return random.nextInt(numPartitions);
        else
            return  Math.abs(key.hashCode()) % numPartitions;
    }
}

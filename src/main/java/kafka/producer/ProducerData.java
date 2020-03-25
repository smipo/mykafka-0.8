package kafka.producer;

import java.util.List;

/**
 * Represents the data to be sent using the Producer send API
 * @param topic the topic under which the message is to be published
 * @param key the key used by the partitioner to pick a broker partition
 * @param data variable length data to be published as Kafka messages under topic
 */
public class ProducerData<K,V> {
    String topic;
    private K key;
    private List<V> data;

    public ProducerData(String topic, K key, List<V> data) {
        this.topic = topic;
        this.key = key;
        this.data = data;
    }

    public ProducerData(String t, List<V> d) {
        this( t, null, d);
    }


    public String getTopic(){
        return topic;
    }

    public K getKey(){
        return key;
    }

    public List<V> getData(){
        return data;
    }
}

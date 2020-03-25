package kafka.javaapi.producer;

import kafka.utils.Lists;

import java.util.List;

public class ProducerData<K,V> {

    private String topic;
    private K key;
    private List<V> data;

    public ProducerData(String topic, K key, List<V> data) {
        this.topic = topic;
        this.key = key;
        this.data = data;
    }
    public ProducerData(String t, List<V> d) {
        this( t,null,  d);
    }

    public ProducerData(String t, V d) {
        this(t, null, Lists.newArrayList(d));
    }

    public String getTopic() {
        return topic;
    }

    public K getKey() {
        return key;
    }

    public List<V> getData() {
        return data;
    }
}

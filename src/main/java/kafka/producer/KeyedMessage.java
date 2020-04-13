package kafka.producer;

public class KeyedMessage<K,V> {

    public String topic;
    public K key;
    public V message;

    public KeyedMessage(String topic, K key, V message) {
        if(topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        this.topic = topic;
        this.key = key;
        this.message = message;
    }
    public KeyedMessage(String topic,V message) {
        this(topic, null, message);
    }

    public boolean hasKey(){
      return key != null;
    }
}

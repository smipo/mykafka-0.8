package kafka.message;

public class MessageAndMetadata<T> {

    private T message;
    private String topic;

    public MessageAndMetadata(){

    }

    public MessageAndMetadata(T message, String topic) {
        this.message = message;
        this.topic = topic;
    }

    public T message() {
        return message;
    }

    public MessageAndMetadata message(T message) {
        this.message = message;
        return this;
    }

    public String topic() {
        return topic;
    }

    public MessageAndMetadata topic(String topic) {
        this.topic = topic;
        return this;
    }
}

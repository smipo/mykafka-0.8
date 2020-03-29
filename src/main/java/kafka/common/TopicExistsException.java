package kafka.common;

public class TopicExistsException extends RuntimeException {
    public TopicExistsException() {
        super();
    }


    public TopicExistsException(String message) {
        super(message);
    }
}

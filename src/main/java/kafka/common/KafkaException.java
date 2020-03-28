package kafka.common;

public class KafkaException extends RuntimeException {

    public KafkaException() {
        super();
    }

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message,Throwable t) {
        super(message,t);
    }
}

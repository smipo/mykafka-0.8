package kafka.common;

public class KafkaStorageException extends RuntimeException {

    public KafkaStorageException() {
        super();
    }

    public KafkaStorageException(String message) {
        super(message);
    }

    public KafkaStorageException(String message,Throwable t) {
        super(message,t);
    }
}

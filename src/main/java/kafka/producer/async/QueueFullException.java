package kafka.producer.async;

public class QueueFullException extends RuntimeException {

    public QueueFullException() {
        super();
    }


    public QueueFullException(String message) {
        super(message);
    }
}

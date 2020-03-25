package kafka.producer.async;

public class QueueClosedException  extends RuntimeException {

    public QueueClosedException() {
        super();
    }


    public QueueClosedException(String message) {
        super(message);
    }
}

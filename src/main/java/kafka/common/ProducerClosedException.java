package kafka.common;

public class ProducerClosedException extends RuntimeException {
    public ProducerClosedException() {
        super();
    }

    public ProducerClosedException(String message) {
        super(message);
    }
}

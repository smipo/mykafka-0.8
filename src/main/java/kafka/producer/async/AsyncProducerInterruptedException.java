package kafka.producer.async;

public class AsyncProducerInterruptedException extends RuntimeException {

    public AsyncProducerInterruptedException() {
        super();
    }


    public AsyncProducerInterruptedException(String message) {
        super(message);
    }
}

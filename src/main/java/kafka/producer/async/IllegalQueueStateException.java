package kafka.producer.async;

public class IllegalQueueStateException extends RuntimeException  {

    public IllegalQueueStateException() {
        super();
    }


    public IllegalQueueStateException(String message) {
        super(message);
    }
}

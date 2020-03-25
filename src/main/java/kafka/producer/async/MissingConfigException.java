package kafka.producer.async;

public class MissingConfigException extends RuntimeException  {

    public MissingConfigException() {
        super();
    }


    public MissingConfigException(String message) {
        super(message);
    }
}

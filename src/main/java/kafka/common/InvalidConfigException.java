package kafka.common;

public class InvalidConfigException extends RuntimeException {

    public InvalidConfigException() {
        super();
    }


    public InvalidConfigException(String message) {
        super(message);
    }
}

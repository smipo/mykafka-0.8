package kafka.common;

public class InvalidMessageSizeException extends RuntimeException {

    public InvalidMessageSizeException() {
        super();
    }


    public InvalidMessageSizeException(String message) {
        super(message);
    }
}

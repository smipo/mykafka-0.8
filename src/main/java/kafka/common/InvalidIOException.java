package kafka.common;

public class InvalidIOException extends RuntimeException {

    public InvalidIOException() {
        super();
    }


    public InvalidIOException(String message) {
        super(message);
    }
}

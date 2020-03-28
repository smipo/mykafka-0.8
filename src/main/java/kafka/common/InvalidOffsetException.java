package kafka.common;

public class InvalidOffsetException extends RuntimeException {

    public InvalidOffsetException() {
        super();
    }


    public InvalidOffsetException(String message) {
        super(message);
    }
}

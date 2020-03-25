package kafka.common;

public class OffsetOutOfRangeException extends RuntimeException{


    public OffsetOutOfRangeException() {
        super();
    }


    public OffsetOutOfRangeException(String message) {
        super(message);
    }

}

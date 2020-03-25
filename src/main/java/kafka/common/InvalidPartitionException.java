package kafka.common;

public class InvalidPartitionException extends RuntimeException {

    public InvalidPartitionException() {
        super();
    }


    public InvalidPartitionException(String message) {
        super(message);
    }
}

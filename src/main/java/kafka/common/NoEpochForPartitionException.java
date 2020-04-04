package kafka.common;

public class NoEpochForPartitionException extends RuntimeException {

    public NoEpochForPartitionException() {
        super();
    }


    public NoEpochForPartitionException(String message) {
        super(message);
    }
}

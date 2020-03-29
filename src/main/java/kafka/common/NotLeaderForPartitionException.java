package kafka.common;

public class NotLeaderForPartitionException extends RuntimeException {

    public NotLeaderForPartitionException() {
        super();
    }


    public NotLeaderForPartitionException(String message) {
        super(message);
    }
}

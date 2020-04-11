package kafka.common;

public class UnknownTopicOrPartitionException extends RuntimeException {

    public UnknownTopicOrPartitionException() {
        super();
    }


    public UnknownTopicOrPartitionException(String message) {
        super(message);
    }
}

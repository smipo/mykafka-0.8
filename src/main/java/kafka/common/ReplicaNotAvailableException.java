package kafka.common;

public class ReplicaNotAvailableException  extends RuntimeException {
    public ReplicaNotAvailableException() {
        super();
    }


    public ReplicaNotAvailableException(String message) {
        super(message);
    }
}

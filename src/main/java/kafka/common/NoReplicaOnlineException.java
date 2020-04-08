package kafka.common;

public class NoReplicaOnlineException  extends RuntimeException {

    public NoReplicaOnlineException() {
        super();
    }


    public NoReplicaOnlineException(String message) {
        super(message);
    }
}

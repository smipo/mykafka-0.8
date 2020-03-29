package kafka.common;

public class LeaderNotAvailableException extends RuntimeException {

    public LeaderNotAvailableException() {
        super();
    }


    public LeaderNotAvailableException(String message) {
        super(message);
    }
}

package kafka.common;

public class StateChangeFailedException extends RuntimeException {

    public StateChangeFailedException() {
        super();
    }


    public StateChangeFailedException(String message) {
        super(message);
    }

    public StateChangeFailedException(String message, Throwable cause) {
        super(message,cause);
    }
}

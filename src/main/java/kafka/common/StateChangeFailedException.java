package kafka.common;

public class StateChangeFailedException extends RuntimeException {

    public StateChangeFailedException() {
        super();
    }


    public StateChangeFailedException(String message) {
        super(message);
    }
}

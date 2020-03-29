package kafka.common;

public class ControllerMovedException  extends RuntimeException {

    public ControllerMovedException() {
        super();
    }


    public ControllerMovedException(String message) {
        super(message);
    }
}

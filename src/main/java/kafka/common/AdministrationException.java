package kafka.common;

public class AdministrationException extends RuntimeException {

    public AdministrationException() {
        super();
    }


    public AdministrationException(String message) {
        super(message);
    }
}

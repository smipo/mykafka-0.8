package kafka.common;

public class UnavailableProducerException  extends RuntimeException {

    public UnavailableProducerException() {
        super();
    }


    public UnavailableProducerException(String message) {
        super(message);
    }
}

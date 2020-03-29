package kafka.common;

public class BrokerNotAvailableException  extends RuntimeException {

    public BrokerNotAvailableException() {
        super();
    }


    public BrokerNotAvailableException(String message) {
        super(message);
    }
}

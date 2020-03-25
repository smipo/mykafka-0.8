package kafka.common;

public class ConsumerRebalanceFailedException  extends RuntimeException  {

    public ConsumerRebalanceFailedException() {
        super();
    }


    public ConsumerRebalanceFailedException(String message) {
        super(message);
    }
}

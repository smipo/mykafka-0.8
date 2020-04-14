package kafka.common;

public class FailedToSendMessageException extends RuntimeException  {

    public FailedToSendMessageException() {
        super();
    }


    public FailedToSendMessageException(String message) {
        super(message);
    }
}

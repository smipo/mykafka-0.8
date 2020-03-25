package kafka.common;

public class MessageSizeTooLargeException extends RuntimeException {

    public MessageSizeTooLargeException() {
        super();
    }


    public MessageSizeTooLargeException(String message) {
        super(message);
    }
}

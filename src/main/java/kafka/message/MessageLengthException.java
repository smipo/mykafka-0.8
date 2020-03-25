package kafka.message;

public class MessageLengthException extends RuntimeException{

    public MessageLengthException() {
        super();
    }


    public MessageLengthException(String message) {
        super(message);
    }
}

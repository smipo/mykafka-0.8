package kafka.common;

public class RequestTimedOutException  extends RuntimeException{
    public RequestTimedOutException() {
        super();
    }


    public RequestTimedOutException(String message) {
        super(message);
    }
}

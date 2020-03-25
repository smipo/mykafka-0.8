package kafka.common;

public class UnknownCodecException extends RuntimeException{

    public UnknownCodecException() {
        super();
    }


    public UnknownCodecException(String message) {
        super(message);
    }
}

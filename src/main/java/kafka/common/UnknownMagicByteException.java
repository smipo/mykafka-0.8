package kafka.common;

public class UnknownMagicByteException extends RuntimeException {

    public UnknownMagicByteException() {
        super();
    }


    public UnknownMagicByteException(String message) {
        super(message);
    }
}

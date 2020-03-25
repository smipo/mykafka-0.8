package kafka.message;

public class MessageAndOffset {

    private Message message;
    private long offset;

    public MessageAndOffset(){

    }

    public MessageAndOffset(Message message,long offset){
        this.message = message;
        this.offset = offset;
    }

    public Message message() {
        return message;
    }

    public MessageAndOffset message(Message message) {
        this.message = message;
        return this;
    }

    public long offset() {
        return offset;
    }

    public MessageAndOffset offset(long offset) {
        this.offset = offset;
        return this;
    }
}

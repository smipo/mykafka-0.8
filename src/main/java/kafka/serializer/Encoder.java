package kafka.serializer;

import kafka.message.Message;

public interface Encoder<T> {

    public Message toMessage(T event);
}

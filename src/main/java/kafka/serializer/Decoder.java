package kafka.serializer;

import kafka.message.Message;

public interface Decoder<T> {

    T toEvent(Message message);
}

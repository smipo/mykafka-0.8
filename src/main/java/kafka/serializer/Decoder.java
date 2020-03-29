package kafka.serializer;


public interface Decoder<T> {

    T fromBytes(byte[] bytes);
}

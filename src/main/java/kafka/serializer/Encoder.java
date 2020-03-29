package kafka.serializer;



public interface Encoder<T> {

    byte[] toBytes(T t);
}

package kafka.serializer;

public class NullEncoder<T> implements  Encoder<T> {

   public byte[] toBytes(T t){
        return null;
    }
}

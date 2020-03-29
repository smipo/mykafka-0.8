package kafka.serializer;


public class DefaultEncoder implements Encoder<byte[]> {

   public byte[] toBytes(byte[] bytes){
        return bytes;
    }
}

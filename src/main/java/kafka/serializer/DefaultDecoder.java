package kafka.serializer;


public class DefaultDecoder implements Decoder<byte[]>{

    public byte[] fromBytes(byte[] bytes){
        return bytes;
    }
}

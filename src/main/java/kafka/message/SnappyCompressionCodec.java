package kafka.message;


public class SnappyCompressionCodec extends CompressionCodec  {

    @Override
    public  int codec(){
        return 2;
    }
    @Override
    public String name(){
        return "snappy";
    }
}

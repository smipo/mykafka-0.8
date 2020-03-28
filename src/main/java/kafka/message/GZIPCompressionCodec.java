package kafka.message;


public class GZIPCompressionCodec extends CompressionCodec {


    @Override
    public  int codec(){
        return 1;
    }
    @Override
    public String name(){
        return "gzip";
    }
}

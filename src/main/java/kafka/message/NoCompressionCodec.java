package kafka.message;



public class NoCompressionCodec extends CompressionCodec {
    @Override
    public  int codec(){
        return 0;
    }
    @Override
    public String name(){
        return "none";
    }
}

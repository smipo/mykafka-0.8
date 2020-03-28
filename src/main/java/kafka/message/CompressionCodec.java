package kafka.message;



public abstract class CompressionCodec {


    public abstract String name();

    public abstract int codec();

}

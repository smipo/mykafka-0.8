package kafka.message;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class NoCompressionCodec extends CompressionCodec {

    public NoCompressionCodec(){
        super();
    }

    public NoCompressionCodec(InputStream inputStream, ByteArrayOutputStream outputStream){
        super(inputStream,outputStream);
    }

    @Override
    public  int codec(){
        return 0;
    }
}

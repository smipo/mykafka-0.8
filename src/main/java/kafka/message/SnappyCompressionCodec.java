package kafka.message;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SnappyCompressionCodec extends CompressionCodec  {

    SnappyInputStream snappyIn;
    SnappyOutputStream snappyOut;

    public SnappyCompressionCodec(){
        super();
    }

    public SnappyCompressionCodec(InputStream inputStream, ByteArrayOutputStream outputStream) throws IOException {
        super(inputStream,outputStream);
        if (inputStream != null) snappyIn = new SnappyInputStream(inputStream);
        if (outputStream == null) snappyOut = new  SnappyOutputStream(outputStream);
    }

    @Override
    public  int codec(){
        return 2;
    }
}

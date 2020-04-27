package kafka.message;

import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionFactory {

    private static Logger logger = Logger.getLogger(CompressionFactory.class);

    public static CompressionCodec getCompressionCodec(int codec) {
        switch (codec){
            case  0:
                return new NoCompressionCodec();
            case 1:
                return new GZIPCompressionCodec();
            case 2:
                return new SnappyCompressionCodec();
            default:
                throw new kafka.common.UnknownCodecException(String.format("%d is an unknown compression codec",codec));
        }
    }
    public static CompressionCodec  getCompressionCodec(String name) {
            if(name.equals("none"))return new NoCompressionCodec();
            else if(name.equals("gzip")) return new GZIPCompressionCodec();
            else if(name.equals("snappy")) return new SnappyCompressionCodec();
            else throw new kafka.common.UnknownCodecException(String.format("%d is an unknown compression name",name));
    }

    public static OutputStream getOutputStream(CompressionCodec compressionCodec, OutputStream stream) throws IOException {
        if(compressionCodec instanceof GZIPCompressionCodec ) return new GZIPOutputStream(stream);
        else if(compressionCodec instanceof SnappyCompressionCodec ) return new SnappyOutputStream(stream);
        else throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }
    public static InputStream getInputStream(CompressionCodec compressionCodec, InputStream stream)  throws IOException{
        if(compressionCodec instanceof GZIPCompressionCodec ) return new GZIPInputStream(stream);
        else if(compressionCodec instanceof SnappyCompressionCodec ) return new SnappyInputStream(stream);
        else throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }

}

package kafka.message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPCompressionCodec extends CompressionCodec {

    GZIPInputStream gzipIn;
    GZIPOutputStream gzipOut;

    public GZIPCompressionCodec(){
        super();
    }
    public GZIPCompressionCodec(InputStream inputStream, ByteArrayOutputStream outputStream) throws IOException{
        super(inputStream,outputStream);
        if (inputStream != null) gzipIn = new  GZIPInputStream(inputStream);
        if (outputStream == null) gzipOut = new GZIPOutputStream(outputStream);
    }
    @Override
    public void close()throws IOException {
        if (gzipIn != null) gzipIn.close();
        if (gzipOut != null) gzipOut.close();
        super.close();
    }
    @Override
    public  int codec(){
        return 1;
    }
    @Override
    public  int read(byte b[]) throws IOException{
        return gzipIn.read(b);
    }
    @Override
    public  void write(byte b[]) throws IOException{
        gzipOut.write(b);
    }
}

package kafka.message;

import kafka.common.InvalidIOException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class CompressionCodec {

    protected InputStream inputStream;
    protected ByteArrayOutputStream outputStream;

    public CompressionCodec(){

    }

    public CompressionCodec(InputStream inputStream,ByteArrayOutputStream outputStream){
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }
    public abstract int codec();
    public int read(byte b[]) throws IOException{
        if(inputStream == null){
            throw new InvalidIOException("compression inputStream is null");
        }
        return inputStream.read(b);
    }
    public void write(byte b[]) throws IOException{
        if(outputStream == null){
            throw new InvalidIOException("compression outputStream is null");
        }
        outputStream.write(b);
    }
    public  void close() throws IOException {
        if (inputStream != null) inputStream.close();
        if (outputStream != null) outputStream.close();
    }
}

package kafka.message;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

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
                throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(String.valueOf(codec)));
        }
    }

    public static CompressionCodec getCompressionCodec(CompressionCodec compressionCodec, ByteArrayOutputStream stream) throws IOException {
        if(compressionCodec instanceof GZIPCompressionCodec ) return new GZIPCompressionCodec(null,stream);
        else if(compressionCodec instanceof SnappyCompressionCodec ) return new SnappyCompressionCodec(null,stream);
        else throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }
    public static CompressionCodec getCompressionCodec(CompressionCodec compressionCodec, InputStream stream)  throws IOException{
        if(compressionCodec instanceof GZIPCompressionCodec ) return new GZIPCompressionCodec(stream,null);
        else if(compressionCodec instanceof SnappyCompressionCodec ) return new SnappyCompressionCodec(stream,null);
        else throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }

    public static Message compress(CompressionCodec compressionCodec,Message...messages) throws IOException{
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        logger.debug("Allocating message byte buffer of size = " + MessageSet.messageSetSize(messages));

        CompressionCodec cf = null;

        if (compressionCodec == null || compressionCodec instanceof NoCompressionCodec)
            cf = getCompressionCodec(new NoCompressionCodec(),outputStream);
        else
            cf = getCompressionCodec(compressionCodec,outputStream);

        ByteBuffer messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
        for(Message message:messages){
            message.serializeTo(messageByteBuffer);
        }
        messageByteBuffer.rewind();

        try {
            cf.write(messageByteBuffer.array());
        } catch (IOException e){
            logger.error("Error while writing to the GZIP output stream", e);
            cf.close();
            throw e;
        } finally {
            cf.close();
        }

        Message oneCompressedMessage = new Message(outputStream.toByteArray(), compressionCodec);
        return oneCompressedMessage;
    }

    public static ByteBufferMessageSet decompress(Message message) throws IOException{

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());

        byte[] intermediateBuffer = new byte[1024];

        CompressionCodec cf = null;

        if (message.compressionCodec() == null || message.compressionCodec() instanceof NoCompressionCodec)
            cf = getCompressionCodec(new NoCompressionCodec(),inputStream);
        else
            cf = getCompressionCodec(message.compressionCodec(),inputStream);

        try {
            int dataRead ;
            while((dataRead = cf.read(intermediateBuffer))!=-1 ){
                outputStream.write(intermediateBuffer, 0, dataRead);
            }
        } catch (IOException e){
            logger.error("Error while reading from the GZIP input stream", e);
            cf.close();
            throw e;
        } finally {
            cf.close();
        }

        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        byte[] outputByteArray = outputStream.toByteArray();
        return new ByteBufferMessageSet(outputBuffer);
    }
}

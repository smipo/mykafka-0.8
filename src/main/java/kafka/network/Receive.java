package kafka.network;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public abstract class Receive extends Transmission{

    private static Logger logger = Logger.getLogger(Receive.class);

    /**
     * Read bytes into this receive from the given channel
     * @param channel The channel to read from
     * @return The number of bytes read
     * @throws IOException If the reading fails
     */
    public abstract long readFrom(ReadableByteChannel channel) throws IOException;

    public abstract ByteBuffer buffer();

    public int readCompletely(ReadableByteChannel channel) throws IOException{
        long read = 0;
        while(!complete()) {
            read = readFrom(channel);
            logger.trace(read + " bytes read.");
        }
        return (int)read;
    }
}

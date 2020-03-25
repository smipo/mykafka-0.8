package kafka.network;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public abstract class Send extends Transmission{

    private static Logger logger = Logger.getLogger(Send.class);

    public long writeCompletely(GatheringByteChannel channel) throws IOException{
        long written = 0;
        while(!complete()) {
            written = writeTo(channel);
            logger.trace(written + " bytes written.");
        }
        return written;
    }


    /**
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The Channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     */
    public abstract long writeTo(GatheringByteChannel channel) throws IOException;
}

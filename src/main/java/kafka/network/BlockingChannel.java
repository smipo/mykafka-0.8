package kafka.network;

import kafka.api.RequestOrResponse;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.*;

/**
 *  A simple blocking channel with timeouts correctly enabled.
 *
 */
public class BlockingChannel {

    private static Logger logger = Logger.getLogger(BlockingChannel.class);

    public static int UseDefaultBufferSize = -1;

    String host;
    int port;
    int readBufferSize;
    int writeBufferSize;
    int readTimeoutMs;

    public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
    }

    private boolean connected = false;
    private SocketChannel channel = null;
    private ReadableByteChannel readChannel = null;
    private GatheringByteChannel writeChannel = null;
    private Object lock = new Object();


    public void connect() throws IOException,SocketException {
         synchronized(lock) {
            if (!connected) {
                channel = SocketChannel.open();
                if (readBufferSize > 0)
                    channel.socket().setReceiveBufferSize(readBufferSize);
                if (writeBufferSize > 0)
                    channel.socket().setSendBufferSize(writeBufferSize);
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(readTimeoutMs);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                channel.connect(new InetSocketAddress(host, port));

                writeChannel = channel;
                readChannel = Channels.newChannel(channel.socket().getInputStream());
                connected = true;
                // settings may not match what we requested above
                String msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d).";
                logger.debug(String.format(msg,channel.socket().getSoTimeout(),
                        readTimeoutMs,
                        channel.socket().getReceiveBufferSize(),
                        readBufferSize,
                        channel.socket().getSendBufferSize(),
                        writeBufferSize));
            }
        }
    }
    public void disconnect() throws IOException{
        synchronized (lock){
            if (connected || channel != null) {
                // closing the main socket channel *should* close the read channel
                // but let's do it to be sure.
                channel.close();
                channel.socket().close();
                readChannel.close();
                channel = null;
                readChannel = null;
                writeChannel = null;
                connected = false;
            }
        }
    }
    public boolean isConnected() {
        return connected;
    }

    public int send(RequestOrResponse request) throws IOException{
        if(!connected)
            throw new ClosedChannelException();

        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
        return (int)send.writeCompletely(writeChannel);
    }

    public Receive receive() throws IOException{
        if(!connected)
            throw new ClosedChannelException();

        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(readChannel);

       return response;
    }
}

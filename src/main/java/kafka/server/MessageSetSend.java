package kafka.server;

import kafka.common.ErrorMapping;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;
import kafka.network.ByteBufferSend;
import kafka.network.Send;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

/**
 * A zero-copy message response that writes the bytes needed directly from the file
 * wholly in kernel space
 */
public class MessageSetSend extends Send {

    private static Logger logger = Logger.getLogger(MessageSetSend.class);

    MessageSet messages;
    int errorCode;

    private long sent = 0;
    private long size ;
    private ByteBuffer header = ByteBuffer.allocate(6);

    private volatile boolean complete;

    public MessageSetSend(MessageSet messages,
            int errorCode){
        this.messages = messages;
        this.errorCode = errorCode;

        this.size = messages.sizeInBytes();
        header.putInt((int)size + 2);
        header.putShort((short) errorCode);
        header.rewind();
    }

    public MessageSetSend(MessageSet messages) {
        this(messages, ErrorMapping.NoError);
    }

    public MessageSetSend(){
        this(new ByteBufferMessageSet(ByteBuffer.allocate(0)));
    }

    public long writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if(header.hasRemaining())
            written += channel.write(header);
        if(!header.hasRemaining()) {
            long fileBytesSent = messages.writeTo(channel, sent, size - sent);
            written += fileBytesSent;
            sent += fileBytesSent;
        }

        if (channel instanceof SocketChannel) {
            SocketChannel socketChannel = (SocketChannel)channel;
            logger.info(sent + " bytes written to " + socketChannel.socket().getRemoteSocketAddress() + " expecting to send " + size + " bytes");
        }

        if(sent >= size)
            complete = true;
        return written;
    }

    public int sendSize(){
       return (int)size + header.capacity();
    }

    public  boolean complete(){
        return complete;
    }
}

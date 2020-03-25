package kafka.message;

import kafka.common.ErrorMapping;
import kafka.common.InvalidMessageSizeException;
import kafka.common.MessageSizeTooLargeException;
import kafka.utils.IteratorTemplate;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 */
public class ByteBufferMessageSet extends MessageSet {

    private static Logger logger = Logger.getLogger(ByteBufferMessageSet.class);

    private ByteBuffer buffer;
    private long initialOffset = 0L;
    private int errorCode = ErrorMapping.NoError;

    private long shallowValidByteCount = -1L;

    public ByteBufferMessageSet(ByteBuffer buffer){
        this(buffer,0L,ErrorMapping.NoError);
    }

    public ByteBufferMessageSet(CompressionCodec compressionCodec, Message ...messages) throws IOException{
        this(MessageSet.createByteBuffer(compressionCodec, messages), 0L, ErrorMapping.NoError);
    }
    public ByteBufferMessageSet(Message...messages) throws IOException{
        this(new NoCompressionCodec(), messages);
    }

    public ByteBufferMessageSet(ByteBuffer buffer,long initialOffset,int errorCode){
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public long getInitialOffset() {
        return initialOffset;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public ByteBuffer serialized() {
        return buffer;
    }

    public long validBytes(){
        return shallowValidBytes();
    }

    private long shallowValidBytes(){
        if(shallowValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while(iter.hasNext()) {
                MessageAndOffset messageAndOffset = iter.next();
                shallowValidByteCount = messageAndOffset.offset();
            }
        }
        if(shallowValidByteCount < initialOffset) return 0;
        else return shallowValidByteCount - initialOffset;
    }

    /** Write the messages in this set to the given channel */
    public long writeTo(GatheringByteChannel channel, long offset, long size) throws IOException {
        buffer.mark();
        long written = channel.write(buffer);
        buffer.reset();
        return written;
    }

    /** default iterator that iterates over decompressed messages */
    public Iterator<MessageAndOffset> iterator(){
        return internalIterator(false);
    }

    /** iterator over compressed messages without decompressing */
    public Iterator<MessageAndOffset> shallowIterator(){
        return internalIterator(true);
    }

    public void verifyMessageSize(int maxMessageSize){
        Iterator<MessageAndOffset> shallowIter = internalIterator(true);
        while(shallowIter.hasNext()){
            MessageAndOffset messageAndOffset = shallowIter.next();
            int payloadSize = messageAndOffset.message().payloadSize();
            if ( payloadSize > maxMessageSize)
                throw new MessageSizeTooLargeException("payload size of " + payloadSize + " larger than " + maxMessageSize);
        }
    }

    /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. This is used in verifyMessageSize() function **/
    private Iterator<MessageAndOffset> internalIterator(final boolean isShallow) {

       return new IteratorTemplate<MessageAndOffset>(){
            ByteBuffer topIter = buffer.slice();
            long currValidBytes = initialOffset;
            Iterator<MessageAndOffset> innerIter = null;
            long lastMessageSize = 0L;

            public boolean innerDone(){
                return (innerIter==null || !innerIter.hasNext());
            }

            public MessageAndOffset makeNextOuter() {
                if (topIter.remaining() < 4) {
                    return allDone();
                }
                int size = topIter.getInt();
                lastMessageSize = size;

                logger.info("Remaining bytes in iterator = " + topIter.remaining());
                logger.info("size of data = " + size);

                if(size < 0 || topIter.remaining() < size) {
                    if (currValidBytes == initialOffset || size < 0)
                        throw new InvalidMessageSizeException("invalid message size: " + size + " only received bytes: " +
                                topIter.remaining() + " at " + currValidBytes + "( possible causes (1) a single message larger than " +
                                "the fetch size; (2) log corruption )");
                    return allDone();
                }
                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                Message newMessage = new Message(message);
                if(!newMessage.isValid())
                    throw new InvalidMessageException("message is invalid, compression codec: " + newMessage.compressionCodec()
                            + " size: " + size + " curr offset: " + currValidBytes + " init offset: " + initialOffset);

                if(isShallow){
                    currValidBytes += 4 + size;
                    logger.trace("shallow iterator currValidBytes = " + currValidBytes);
                    return new MessageAndOffset(newMessage, currValidBytes);
                }
                else{
                    if(newMessage.compressionCodec() instanceof NoCompressionCodec){
                        logger.debug("Message is uncompressed. Valid byte count = %d".format(String.valueOf(currValidBytes)));
                        innerIter = null;
                        currValidBytes += 4 + size;
                        logger.trace("currValidBytes = " + currValidBytes);
                        return new MessageAndOffset(newMessage, currValidBytes);
                    }else{
                        logger.debug("Message is compressed. Valid byte count = %d".format(String.valueOf(currValidBytes)));
                        try {
                            innerIter = CompressionFactory.decompress(newMessage).internalIterator(false);
                        }catch (IOException e){
                            logger.error("message decompress Error:",e);
                            throw new RuntimeException("message decompress Error:"+e.getMessage());
                        }

                        if (!innerIter.hasNext()) {
                            currValidBytes += 4 + lastMessageSize;
                            innerIter = null;
                        }
                        return  makeNext();
                    }
                }
            }

            public MessageAndOffset makeNext() {
                if(isShallow){
                    return makeNextOuter();
                }
                else{
                    boolean isInnerDone = innerDone();
                    logger.debug("makeNext() in internalIterator: innerDone = " + isInnerDone);
                    if(isInnerDone){
                        return makeNextOuter();
                    }else{
                        MessageAndOffset messageAndOffset = innerIter.next();
                        if (!innerIter.hasNext())
                            currValidBytes += 4 + lastMessageSize;
                        return new MessageAndOffset(messageAndOffset.message(), currValidBytes);
                    }
                }
            }
        };
    }

    public long sizeInBytes(){
        return buffer.limit();
    }



    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ByteBufferMessageSet){
            ByteBufferMessageSet that = (ByteBufferMessageSet)obj;
            return errorCode == that.errorCode && buffer.equals(that.buffer) && initialOffset == that.initialOffset;
        }
        return false;
    }


    @Override
    public int hashCode(){
        return 31 + (17 * errorCode) + buffer.hashCode() + (int)initialOffset;
    }

}

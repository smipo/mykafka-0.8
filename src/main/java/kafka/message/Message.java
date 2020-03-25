package kafka.message;

import kafka.common.UnknownMagicByteException;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;

/**
 * A message. The format of an N byte message is the following:
 *
 * If magic byte is 0
 *
 * 1. 1 byte "magic" identifier to allow format changes
 *
 * 2. 4 byte CRC32 of the payload
 *
 * 3. N - 5 byte payload
 *
 * If magic byte is 1
 *
 * 1. 1 byte "magic" identifier to allow format changes
 *
 * 2. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 *
 * 3. 4 byte CRC32 of the payload
 *
 * 4. N - 6 byte payload
 *
 */
public class Message {

    private static Logger logger = Logger.getLogger(Message.class);

    public static byte MagicVersion1 = 0;
    public static byte MagicVersion2 = 1;
    public static byte CurrentMagicValue = 1;
    public static int MagicOffset = 0;
    public static int MagicLength = 1;
    public static int AttributeOffset = MagicOffset + MagicLength;
    public static int AttributeLength = 1;
    /**
     * Specifies the mask for the compression code. 2 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    public static int CompressionCodeMask = 0x03;  //


    public static int NoCompression = 0;

    /**
     * Computes the CRC value based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int crcOffset(byte magic){
        if(magic == MagicVersion1){
            return MagicOffset + MagicLength;
        }else if(magic == MagicVersion2){
            return AttributeOffset + AttributeLength;
        }else{
            throw new UnknownMagicByteException("Magic byte value of %d is unknown".format(String.valueOf(magic)));
        }
    }

    public static int CrcLength = 4;

    /**
     * Computes the offset to the message payload based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int payloadOffset(byte magic){
        return crcOffset(magic) + CrcLength;
    }

    /**
     * Computes the size of the message header based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int headerSize(byte magic){
        return payloadOffset(magic);
    }

    /**
     * Size of the header for magic byte 0. This is the minimum size of any message header
     */
    public static int MinHeaderSize = headerSize((byte)0);

    protected ByteBuffer buffer;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private Message(long checksum, byte[] bytes, CompressionCodec compressionCodec){
        this(ByteBuffer.allocate(Message.headerSize(Message.CurrentMagicValue) + bytes.length));
        buffer.put(CurrentMagicValue);
        byte attributes = 0;
        if (compressionCodec.codec() > 0) {
            attributes =  (byte)(attributes | (Message.CompressionCodeMask & compressionCodec.codec()));
        }
        buffer.put(attributes);
        Utils.putUnsignedInt(buffer, checksum);
        buffer.put(bytes);
        buffer.rewind();
        logger.info("Message content:"+new String(bytes));
    }

    public Message(long checksum, byte[] bytes) {
        this(checksum, bytes, new NoCompressionCodec());
    }

    public Message(byte[] bytes, CompressionCodec compressionCodec)  {
        //Note: we're not crc-ing the attributes header, so we're susceptible to bit-flipping there
        this(Utils.crc32(bytes), bytes, compressionCodec);
    }

    public Message(byte[] bytes) {
        this(bytes, new NoCompressionCodec());
    }

    public int size(){
        return buffer.limit();
    }

    public int payloadSize(){
        return size() - headerSize(magic());
    }

    public byte magic(){
        return buffer.get(MagicOffset);
    }
    public byte attributes(){
        return buffer.get(AttributeOffset);
    }

    public CompressionCodec compressionCodec() {
        switch (magic()){
            case 0:
                return new NoCompressionCodec();
            case 1:
               return CompressionFactory.getCompressionCodec(buffer.get(AttributeOffset) & CompressionCodeMask);
            default:
                throw new RuntimeException("Invalid magic byte " + magic());
        }
    }

    public long checksum(){
        return Utils.getUnsignedInt(buffer, crcOffset(magic()));
    }

    public ByteBuffer  payload(){
        ByteBuffer payload = buffer.duplicate();
        payload.position(headerSize(magic()));
        payload = payload.slice();
        payload.limit(payloadSize());
        payload.rewind();
        return payload;
    }

    public boolean isValid(){
        return checksum() == Utils.crc32(buffer.array(), buffer.position() + buffer.arrayOffset() + payloadOffset(magic()), payloadSize());
    }

    public int serializedSize(){
        return 4 /* int size*/ + buffer.limit();
    }
    public void serializeTo(ByteBuffer serBuffer) {
        serBuffer.putInt(buffer.limit());
        serBuffer.put(buffer.duplicate());
    }
    @Override
    public String toString(){
        return  "message(magic = %d, attributes = %d, crc = %d, payload = %s)".format(String.valueOf(magic()), attributes(), checksum(), payload());
    }
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Message){
            Message that = (Message)obj;
            return size() == that.size() && attributes() == that.attributes() && checksum() == that.checksum() &&
                    payload() == that.payload() && magic() == that.magic();
        }
        return false;

    }
    @Override
    public int hashCode(){
        return buffer.hashCode();
    }
}

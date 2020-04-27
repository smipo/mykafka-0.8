package kafka.message;

import kafka.common.UnknownMagicByteException;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K
 * 5. K byte key
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 */
public class Message {

    private static Logger logger = Logger.getLogger(Message.class);



    /**
     * The current offset and size for all the fixed-length fields
     */
    public static int CrcOffset = 0;
    public static int  CrcLength = 4;
    public static int  MagicOffset = CrcOffset + CrcLength;
    public static int  MagicLength = 1;
    public static int  AttributesOffset = MagicOffset + MagicLength;
    public static int  AttributesLength = 1;
    public static int  KeySizeOffset = AttributesOffset + AttributesLength;
    public static int  KeySizeLength = 4;
    public static int  KeyOffset = KeySizeOffset + KeySizeLength;
    public static int  ValueSizeLength = 4;

    /** The amount of overhead bytes in a message */
    public static int  MessageOverhead = KeyOffset + ValueSizeLength;

    /**
     * The minimum valid size for the message header
     */
    public static int  MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;

    /**
     * The current "magic" value
     */
    public static byte  CurrentMagicValue = 0;

    /**
     * Specifies the mask for the compression code. 2 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    public static int  CompressionCodeMask = 0x03;

    /**
     * Compression code for uncompressed messages
     */
    public static int  NoCompression = 0;


    protected ByteBuffer buffer;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
    }


    public Message(byte[] bytes, byte[] key,CompressionCodec codec,int payloadOffset,
                    int payloadSize){
        this(ByteBuffer.allocate(Message.CrcLength +
                Message.MagicLength +
                Message.AttributesLength +
                Message.KeySizeLength +
                (key == null ? 0 : key.length) +
                Message.ValueSizeLength +
                (payloadSize >= 0 ? payloadSize : bytes.length - payloadOffset)));
        // skip crc, we will fill that in at the end
        buffer.position(MagicOffset);
        buffer.put(CurrentMagicValue);
        byte attributes = 0;
        if (codec.codec() > 0)
            attributes =  (byte)(attributes | (CompressionCodeMask & codec.codec()));
        buffer.put(attributes);
        if(key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key, 0, key.length);
        }
        int size = payloadSize >= 0 ? payloadSize : bytes.length - payloadOffset;
        buffer.putInt(size);
        buffer.put(bytes, payloadOffset, size);
        buffer.rewind();

        // now compute the checksum and fill it in
        Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum());
    }

    public Message(byte[] bytes, byte[] key, CompressionCodec codec) {
        this( bytes,  key, codec,  0, -1);
    }

    public Message(byte[] bytes, CompressionCodec codec) {
        this(bytes,  null, codec);
    }

    public Message(byte[] bytes, byte[] key) {
        this(bytes,  key,new NoCompressionCodec());
    }

    public Message(byte[] bytes) {
        this(bytes,  null, new NoCompressionCodec());
    }



    /**
     * Compute the checksum of the message from the message contents
     */
    public long computeChecksum() {
       return Utils.crc32(buffer.array(), buffer.arrayOffset() + MagicOffset, buffer.limit() - MagicOffset);
    }

    /**
     * Retrieve the previously computed CRC for this message
     */
    public long  checksum(){
        return  Utils.readUnsignedInt(buffer, CrcOffset);
    }

    /**
     * Returns true if the crc stored with the message matches the crc computed off the message contents
     */
    public boolean isValid(){
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidMessageException if isValid is false for this message
     */
    public void ensureValid() {
        if(!isValid())
            throw new InvalidMessageException("Message is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * The complete serialized size of this message in bytes (including crc, header attributes, etc)
     */
    public int size(){
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize(){
        return buffer.getInt(Message.KeySizeOffset);
    }

    /**
     * Does the message have a key?
     */
    public boolean hasKey(){
        return keySize() >= 0;
    }

    /**
     * The position where the payload size is stored
     */
    private int payloadSizeOffset (){
        return Message.KeyOffset + Math.max(0, keySize());
    }

    /**
     * The length of the message value in bytes
     */
    public int payloadSize(){
        return buffer.getInt(payloadSizeOffset());
    }

    /**
     * The magic version of this message
     */
    private byte magic(){
        return buffer.get(MagicOffset);
    }

    /**
     * The attributes stored with this message
     */
    private byte attributes(){
        return buffer.get(AttributesOffset);
    }

    /**
     * The compression codec used with this message
     */
    public CompressionCodec compressionCodec() {
        return CompressionFactory.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask);
    }

    /**
     * A ByteBuffer containing the content of the message
     */
    public ByteBuffer payload (){
        return sliceDelimited(payloadSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key(){
        return sliceDelimited(KeySizeOffset);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start){
        int size = buffer.getInt(start);
        if(size < 0) {
           return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return  b;
        }
    }

    @Override
    public String toString() {
        return String.format("Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)",magic(), attributes(), checksum(), key(), payload());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(buffer, message.buffer);
    }

    @Override
    public int hashCode(){
        return buffer.hashCode();
    }
}

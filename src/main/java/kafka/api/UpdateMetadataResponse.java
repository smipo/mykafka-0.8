package kafka.api;

import java.io.IOException;
import java.nio.ByteBuffer;

public class UpdateMetadataResponse  extends RequestOrResponse{

    public static UpdateMetadataResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        return new UpdateMetadataResponse(correlationId, errorCode);
    }

    public short errorCode;

    public UpdateMetadataResponse( int correlationId,short errorCode) {
        super(null,correlationId);
        this.errorCode = errorCode;
    }

    public int sizeInBytes() throws IOException {
        return 4 /* correlation id */ + 2; /* error code */
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
    }
}

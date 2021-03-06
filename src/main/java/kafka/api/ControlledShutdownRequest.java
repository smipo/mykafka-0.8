package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

public class ControlledShutdownRequest  extends RequestOrResponse {

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";

    public static ControlledShutdownRequest  readFrom(ByteBuffer buffer){
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        int brokerId = buffer.getInt();
        return new ControlledShutdownRequest(versionId, correlationId, brokerId);
    }

    public short versionId;
    public int brokerId;

    public ControlledShutdownRequest( short versionId, int correlationId,int brokerId) {
        super(RequestKeys.ControlledShutdownKey, correlationId);
        this.versionId = versionId;
        this.brokerId = brokerId;
    }

    public ControlledShutdownRequest(int correlationId, int brokerId) {
        this(ControlledShutdownRequest.CurrentVersion, correlationId, brokerId);
    }

    public void writeTo(ByteBuffer buffer)throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        buffer.putInt(brokerId);
    }

    public int sizeInBytes() {
       return  2 +  /* version id */
                4 + /* correlation id */
                4; /* broker id */
    }


    @Override
    public String toString() {
        StringBuilder controlledShutdownRequest = new StringBuilder();
        controlledShutdownRequest.append("Name: " + this.getClass().getSimpleName());
        controlledShutdownRequest.append("; Version: " + versionId);
        controlledShutdownRequest.append("; CorrelationId: " + correlationId);
        controlledShutdownRequest.append("; BrokerId: " + brokerId);
        return controlledShutdownRequest.toString();
    }

    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) throws IOException,InterruptedException{
        ControlledShutdownResponse errorResponse = new ControlledShutdownResponse(correlationId, ErrorMapping.codeFor(e.getCause().getClass().getName()), new HashSet<>());
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}

package kafka.api;

import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.nio.ByteBuffer;

public class ControlledShutdownRequest  extends RequestOrResponse {

    short versionId;
    int brokerId;

    public ControlledShutdownRequest( int correlationId, short versionId, int correlationId1, int brokerId) {
        super(RequestKeys.ControlledShutdownKey, correlationId);
        this.versionId = versionId;
        this.brokerId = brokerId;
    }

    public ControlledShutdownRequest(int correlationId, int brokerId) {
        this(ControlledShutdownRequest.CurrentVersion, correlationId, brokerId)
    }

    public void writeTo(ByteBuffer buffer) {
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

    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        val errorResponse = ControlledShutdownResponse(correlationId, ErrorMapping.codeFor(e.getCause().getClass().getName()), Set.empty[TopicAndPartition])
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend (errorResponse)));
    }
}

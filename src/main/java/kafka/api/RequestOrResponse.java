package kafka.api;

import kafka.network.RequestChannel;

import java.nio.ByteBuffer;

public abstract class RequestOrResponse {
    public Short requestId;
    public  int correlationId;

    public RequestOrResponse(Short requestId, int correlationId) {
        this.requestId = requestId;
        this.correlationId = correlationId;
    }

    public abstract int sizeInBytes();

    public abstract void writeTo(ByteBuffer buffer);

    public abstract void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request);
}

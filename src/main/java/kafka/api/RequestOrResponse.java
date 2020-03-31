package kafka.api;

import kafka.network.RequestChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class RequestOrResponse {

    public static int OrdinaryConsumerId = -1;
    public static int DebuggingConsumerId = -2;

    public Short requestId;
    public  int correlationId;

    public RequestOrResponse(Short requestId, int correlationId) {
        this.requestId = requestId;
        this.correlationId = correlationId;
    }

    public abstract int sizeInBytes() throws IOException;

    public abstract void writeTo(ByteBuffer buffer) throws IOException;

    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{

    }
}

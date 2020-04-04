package kafka.network;

import kafka.api.RequestKeys;
import kafka.api.RequestOrResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestChannel {

    int numProcessors;
    int queueSize;
    SocketServer.Processor[] processors;

    public RequestChannel(int numProcessors, int queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;
        requestQueue = new ArrayBlockingQueue<>(queueSize);
        responseQueues = new BlockingQueue[numProcessors];
        for(int i = 0;i < numProcessors; i++ )
            responseQueues[i] = new LinkedBlockingQueue<RequestChannel.Response>();
    }

    private ArrayBlockingQueue<RequestChannel.Request> requestQueue ;
    private BlockingQueue<Response>[] responseQueues ;


    /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
    public void sendRequest(RequestChannel.Request request)throws InterruptedException {
        requestQueue.put(request);
    }


    /** Get the next request or block until there is one */
    public RequestChannel.Request receiveRequest() throws InterruptedException{
        return requestQueue.take();
    }

    /** Get a response for the given processor if there is one */
    public RequestChannel.Response receiveResponse(int processor) {
        return responseQueues[processor].poll();
    }

    /** Send a response back to the socket server to be sent over the network */
    public void sendResponse(RequestChannel.Response response) throws InterruptedException{
        responseQueues[response.processor].put(response);
        processors[response.processor].wakeup();
    }

    /** No operation to take for the request, need to read more over the network */
    public void noOperation(int processor,RequestChannel.Request request) throws InterruptedException {
        responseQueues[processor].put(new RequestChannel.Response(processor, request, null, new RequestChannel.NoOpAction()));
        processors[processor].wakeup();
    }

    /** Close the connection for the request */
    public void closeConnection(int processor,RequestChannel.Request request) throws InterruptedException {
        responseQueues[processor].put(new RequestChannel.Response(processor, request, null, new RequestChannel.CloseConnectionAction()));
        processors[processor].wakeup();
    }

    public void addResponseListener(SocketServer.Processor[] processors){
        this.processors = processors;
    }

    public void shutdown() {
        requestQueue.clear();
    }


    public static class Request{

        public int processor;
        public Object requestKey;
        private ByteBuffer buffer;
        public  long startTimeMs;
        public SocketAddress remoteAddress ;
        public short requestId ;
        public RequestOrResponse requestObj;

        public Request(int processor, Object requestKey, ByteBuffer buffer, long startTimeMs, SocketAddress remoteAddress) throws IOException {
            this.processor = processor;
            this.requestKey = requestKey;
            this.buffer = buffer;
            this.startTimeMs = startTimeMs;
            this.remoteAddress = remoteAddress;

            requestId = buffer.getShort();
            requestObj = RequestKeys.deserializerForKey(requestId,buffer);
        }
    }

    public  static abstract class ResponseAction{

    }
    public static class SendAction extends ResponseAction{

    }
    public static class NoOpAction extends ResponseAction{

    }
    public static class CloseConnectionAction extends ResponseAction{

    }

    public static class Response{
        public int processor;
        public Request request;
        public Send responseSend;
        public ResponseAction responseAction;

        public Response(int processor, Request request, Send responseSend, ResponseAction responseAction) {
            this.processor = processor;
            this.request = request;
            this.responseSend = responseSend;
            this.responseAction = responseAction;
        }

        public Response(int processor,  Request request, Send responseSend) {
            this(processor, request, responseSend, responseSend == null?new NoOpAction():new SendAction());
        }

        public Response(Request request,Send send) {
            this(request.processor, request, send);
        }
    }
}

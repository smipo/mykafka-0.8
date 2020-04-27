package kafka.network;

import kafka.common.KafkaException;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketServer {

    private static Logger logger = Logger.getLogger(SocketServer.class);

    int brokerId;
    String host;
    int maxQueuedRequests;
    int port;
    int numProcessorThreads;
    int sendBufferSize;
    int recvBufferSize;
    int maxRequestSize = Integer.MAX_VALUE;

    public SocketServer(int brokerId, String host, int port, int numProcessorThreads,int maxQueuedRequests,  int sendBufferSize, int recvBufferSize, int maxRequestSize) {
        this.brokerId = brokerId;
        this.host = host;
        this.maxQueuedRequests = maxQueuedRequests;
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.maxRequestSize = maxRequestSize;

        processors = new Processor[numProcessorThreads];
        requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests);
    }

    private Processor[] processors;
    private volatile Acceptor acceptor;
    public RequestChannel requestChannel ;
    public long milliseconds = System.currentTimeMillis();

    /**
     * Start the socket server
     */
    public void startup() throws IOException,InterruptedException{
        for(int i = 0 ;i < numProcessorThreads; i++) {
            processors[i] = new Processor(i, milliseconds, maxRequestSize, requestChannel);
            Utils.newThread("kafka-processor-" + i, processors[i], false).start();
        }
        // register the processor threads for notification of responses
        requestChannel.addResponseListener(processors);
        // start accepting connections
        this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize);
        Utils.newThread("kafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
        logger.info("Started");
    }

    /**
     * Shutdown the socket server
     */
    public void shutdown() throws InterruptedException{
        if(acceptor != null) acceptor.shutdown();
        for(Processor processor : processors)
            processor.shutdown();
    }


    /**
     * A base class with some helper variables and methods
     */
    public abstract class AbstractServerThread implements Runnable {

        protected Selector selector ;
        private CountDownLatch startupLatch = new CountDownLatch(1);
        private CountDownLatch shutdownLatch = new CountDownLatch(1);
        private AtomicBoolean alive = new AtomicBoolean(false);

        public AbstractServerThread() throws IOException {
            selector = Selector.open();
        }

        /**
         * Initiates a graceful shutdown by signeling to stop and waiting for the shutdown to complete
         */
        public void shutdown() throws InterruptedException {
            alive.set(false);
            selector.wakeup();
            shutdownLatch.await();
        }

        /**
         * Wait for the thread to completely start up
         */
        public void awaitStartup() throws InterruptedException {
            startupLatch.await();
        }

        /**
         * Record that the thread startup is complete
         */
        public void startupComplete()  {
            alive.set(true);
            startupLatch.countDown();
        }

        /**
         * Record that the thread shutdown is complete
         */
        public void shutdownComplete() {
            shutdownLatch.countDown();
        }

        /**
         * Is the server still running?
         */
        public boolean isRunning() {
            return alive.get();
        }

        /**
         * Wakeup the thread for selection.
         */
        public Selector  wakeup() {
            return selector.wakeup();
        }
    }


    /**
     * Thread that accepts and configures new connections. There is only need for one of these
     */
    class Acceptor extends AbstractServerThread {

        private String host;
        private int port;
        private Processor[] processors;
        private int sendBufferSize;
        private int receiveBufferSize;

        public Acceptor( String host,int port,Processor[] processors,int sendBufferSize,int receiveBufferSize)throws IOException {
            super();
            this.host = host;
            this.port = port;
            this.processors = processors;
            this.sendBufferSize = sendBufferSize;
            this.receiveBufferSize = receiveBufferSize;
            this.serverChannel = openServerSocket(host,  port);
        }

        ServerSocketChannel serverChannel;
        /**
         * Accept loop that checks for new connection attempts
         */
        public void run() {
            try{
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);
                logger.info("Awaiting connections on port " + port);
                startupComplete();

                int currentProcessor = 0;
                while(isRunning()) {
                    int ready = selector.select(500);
                    if(ready > 0) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        Iterator<SelectionKey> iter = keys.iterator();
                        while(iter.hasNext() && isRunning()) {
                            SelectionKey key = null;
                            try {
                                key = iter.next();
                                iter.remove();
                                if(key.isAcceptable())
                                    accept(key, processors[currentProcessor]);
                                else
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");

                                // round robin to the next processor thread
                                currentProcessor = (currentProcessor + 1) % processors.length;
                            } catch (Exception e){
                                logger.error("Error in acceptor", e);
                            }
                        }
                    }
                }
                logger.debug("Closing server socket and selector.");
                serverChannel.close();
                selector.close();
            }catch (IOException e){
                logger.error("Error IOException in acceptor" , e);
            }
            shutdownComplete();
        }

        /*
         * Create a server socket to listen for connections on.
         */
      public ServerSocketChannel openServerSocket(String host, int port) throws IOException{
          InetSocketAddress socketAddress ;
            if(host == null || host.trim().isEmpty())
                socketAddress = new InetSocketAddress(port);
            else
                socketAddress = new InetSocketAddress(host, port);
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            try {
                serverChannel.socket().bind(socketAddress);
                logger.info(String.format("Awaiting socket connections on %s:%d.",socketAddress.getHostName(), port));
            } catch (SocketException e){
                    throw new KafkaException(String.format("Socket server failed to bind to %s:%d: %s.",socketAddress.getHostName(), port, e.getMessage()), e);
            }
            return serverChannel;
        }
        /*
         * Accept a new connection
         */
        public void accept(SelectionKey key, Processor processor) throws IOException{
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
            serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSendBufferSize(sendBufferSize);

            if (logger.isDebugEnabled()) {
                logger.debug("sendBufferSize: [" + socketChannel.socket().getSendBufferSize()
                        + "] receiveBufferSize: [" + socketChannel.socket().getReceiveBufferSize() + "]");
            }
            processor.accept(socketChannel);
        }
    }

    /**
     * Thread that processes all requests from a single connection. There are N of these running in parallel
     * each of which has its own selectors
     */
   public class Processor extends AbstractServerThread{

        private int id;
        private long milliseconds;
        private int maxRequestSize;
        private RequestChannel requestChannel;

        private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();

        public Processor(int id, long milliseconds,int maxRequestSize, RequestChannel requestChannel) throws IOException {
            this.id = id;
            this.milliseconds = milliseconds;
            this.maxRequestSize = maxRequestSize;
            this.requestChannel = requestChannel;
        }

        public void run() {
            startupComplete();
            try{
                while (isRunning()) {
                    // setup any new connections that have been queued up
                    configureNewConnections();
                    // register any new responses for writing
                    processNewResponses();
                    int ready = selector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        Iterator<SelectionKey> iter = keys.iterator();
                        while (iter.hasNext() && isRunning()) {
                            SelectionKey key = null;
                            try {
                                key = iter.next();
                                iter.remove();

                                if (key.isReadable())
                                    read(key);
                                else if (key.isWritable())
                                    write(key);
                                else if (!key.isValid())
                                    close(key);
                                else
                                    throw new IllegalStateException("Unrecognized key state for processor thread.");
                            } catch (EOFException e){
                                logger.info(String.format("Closing socket connection to %s.",channelFor(key).socket().getInetAddress().toString()));
                                close(key);
                            }catch (InvalidRequestException e){
                                logger.info(String.format("Closing socket connection to %s due to invalid request: %s",channelFor(key).socket().getInetAddress().toString(), e.getMessage()));
                                close(key);
                            }catch (Throwable e){
                                logger.error("Closing socket for " + channelFor(key).socket().getInetAddress() + " because of error", e);
                                close(key);
                            }
                        }
                    }
                }
            }catch (IOException e){
                logger.error("Error IOException in Processor" , e);
            }
            logger.debug("Closing selector.");
            try{
                selector.close();
            }catch (IOException e){
                logger.error("Error selector close" , e);
            }

            shutdownComplete();
        }

        private void processNewResponses() throws IOException{
            RequestChannel.Response curr = requestChannel.receiveResponse(id);
            while(curr != null) {
                SelectionKey key = (SelectionKey)curr.request.requestKey;
                try {
                    if(curr.responseAction instanceof RequestChannel.NoOpAction){
                        logger.trace("Socket server received empty response to send, registering for read: " + curr);
                        key.interestOps(SelectionKey.OP_READ);
                        key.attach(null);
                    }else if(curr.responseAction instanceof RequestChannel.SendAction){
                        logger.trace("Socket server received response to send, registering for write: " + curr);
                        key.interestOps(SelectionKey.OP_WRITE);
                        key.attach(curr);
                    }else if(curr.responseAction instanceof RequestChannel.CloseConnectionAction){
                        logger.trace("Closing socket connection actively according to the response code.");
                        close(key);
                    }else{
                        throw new KafkaException("No mapping found for response code " + curr.responseAction);
                    }
                } catch (CancelledKeyException e){
                    logger.debug("Ignoring response for closed socket.");
                    close(key);
                } finally {
                    curr = requestChannel.receiveResponse(id);
                }
            }
        }


        private void close(SelectionKey key) throws IOException{
            SocketChannel channel = (SocketChannel)key.channel();
            if (logger.isDebugEnabled())
                logger.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
            channel.socket().close();
            channel.close();
            key.attach(null);
            key.cancel();
        }

        /**
         * Queue up a new connection for reading
         */
        private void accept (SocketChannel socketChannel){
            newConnections.add(socketChannel);
            selector.wakeup();
        }

        /**
         * Register any new connections that have been queued up
         */
        private void configureNewConnections () throws ClosedChannelException {
            while (newConnections.size() > 0) {
                SocketChannel channel = newConnections.poll();
                if (logger.isDebugEnabled())
                    logger.debug("Listening to new connection from " + channel.socket().getRemoteSocketAddress());
                channel.register(selector, SelectionKey.OP_READ);
            }
        }


        /*
         * Process reads from ready sockets
         */
        private void read(SelectionKey key) throws Exception{
            SocketChannel socketChannel = channelFor(key);
            Receive request = null;
            if (key.attachment() == null) {
                request = new BoundedByteBufferReceive(maxRequestSize);
                key.attach(request);
            }else{
                request = (Receive)key.attachment();
            }
            long read = request.readFrom(socketChannel);
            if (logger.isTraceEnabled())
                logger.trace(read + " bytes read from " + socketChannel.socket().getRemoteSocketAddress());
            if (read < 0) {
                close(key);
                return;
            } else if (request.complete()) {
                SocketAddress address = socketChannel.socket().getRemoteSocketAddress();
                RequestChannel.Request req = new RequestChannel.Request( id,  key,  request.buffer(), milliseconds, address);
                requestChannel.sendRequest(req);
                key.attach(null);
                // explicitly reset interest ops to not READ, no need to wake up the selector just yet
                key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
            } else {
                // more reading to be done
                key.interestOps(SelectionKey.OP_READ);
                selector.wakeup();
            }
        }

        /*
         * Process writes to ready sockets
         */
        private void write(SelectionKey key) throws Exception{
            Send response = (Send)key.attachment();
            SocketChannel socketChannel = channelFor(key);
            long written = response.writeTo(socketChannel);
            if (logger.isTraceEnabled())
                logger.info(written + " bytes written to " + socketChannel.socket().getRemoteSocketAddress());
            if (response.complete()) {
                key.attach(null);
                key.interestOps(SelectionKey.OP_READ);
            } else {
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }

        private SocketChannel channelFor (SelectionKey key) {
            return (SocketChannel)key.channel();
        }
    }
}

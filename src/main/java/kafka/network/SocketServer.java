package kafka.network;

import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketServer {

    private static Logger logger = Logger.getLogger(SocketServer.class);

    int port;
    int numProcessorThreads;
    int monitoringPeriodSecs;
    private Handler handlerFactory;
    int sendBufferSize;
    int receiveBufferSize;
    int maxRequestSize = Integer.MAX_VALUE;

    public SocketServer(int port,
            int numProcessorThreads,
            int monitoringPeriodSecs,
             Handler handlerFactory,
            int sendBufferSize,
            int receiveBufferSize,
            int maxRequestSize )throws IOException{
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.monitoringPeriodSecs = monitoringPeriodSecs;
        this.handlerFactory = handlerFactory;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.maxRequestSize = maxRequestSize;

        processors = new Processor[numProcessorThreads];
        acceptor = new Acceptor(port, processors, sendBufferSize, receiveBufferSize);
    }

    private Processor[] processors;
    private Acceptor acceptor;

    /**
     * Start the socket server
     */
    public void startup() throws IOException,InterruptedException{
        for(int i = 0 ;i < numProcessorThreads; i++) {
            processors[i] = new Processor(handlerFactory,  maxRequestSize);
            Utils.newThread("kafka-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("kafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
    }

    /**
     * Shutdown the socket server
     */
    public void shutdown() throws InterruptedException{
        acceptor.shutdown();
        for(Processor processor : processors)
            processor.shutdown();
    }


    /**
     * A base class with some helper variables and methods
     */
    abstract class AbstractServerThread implements Runnable {

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
        protected void startupComplete()  {
            alive.set(true);
            startupLatch.countDown();
        }

        /**
         * Record that the thread shutdown is complete
         */
        protected void shutdownComplete() {
            shutdownLatch.countDown();
        }

        /**
         * Is the server still running?
         */
        protected boolean isRunning() {
            return alive.get();
        }
    }


    /**
     * Thread that accepts and configures new connections. There is only need for one of these
     */
    class Acceptor extends AbstractServerThread {

        private int port;
        private Processor[] processors;
        private int sendBufferSize;
        private int receiveBufferSize;

        public Acceptor(int port,Processor[] processors,int sendBufferSize,int receiveBufferSize)throws IOException {
            super();
            this.port = port;
            this.processors = processors;
            this.sendBufferSize = sendBufferSize;
            this.receiveBufferSize = receiveBufferSize;
        }
        /**
         * Accept loop that checks for new connection attempts
         */
        public void run() {
            try{
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                serverChannel.socket().bind(new InetSocketAddress(port));
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
    class Processor extends AbstractServerThread{

        private Handler handlerMapping;
        private int maxRequestSize;

        private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();

        public Processor(Handler handlerMapping,int maxRequestSize) throws IOException{
            super();
            this.handlerMapping = handlerMapping;
            this.maxRequestSize = maxRequestSize;
        }

        public void run() {
            startupComplete();
            try{
                while (isRunning()) {
                    // setup any new connections that have been queued up
                    configureNewConnections();
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
                                logger.info("Closing socket connection to %s.".format(channelFor(key).socket().getInetAddress().toString()));
                                close(key);
                            }catch (InvalidRequestException e){
                                logger.info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket().getInetAddress().toString(), e.getMessage()));
                                close(key);
                            }catch (Throwable e){
                                logger.error("Closing socket for " + channelFor(key).socket().getInetAddress() + " because of error", e);
                                close(key);
                            }
                        }
                    }
                }
                logger.debug("Closing selector.");
                selector.close();
            }catch (IOException e){
                logger.error("Error IOException in Processor" , e);
            }
            shutdownComplete();
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

        /**
         * Handle a completed request producing an optional response
         */
        private Send handle(SelectionKey key, Receive request) throws Exception{
            //Todo
            //if(handlerMapping == null) return null;
            short requestTypeId = request.buffer().getShort();
            Send send = handlerMapping.handler(requestTypeId, request);
            return send;
        }

        /*
         * Process reads from ready sockets
         */
        private void read (SelectionKey key) throws Exception{
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
                Send maybeResponse = handle(key, request);
                key.attach(null);
                if(maybeResponse != null){
                    key.attach(maybeResponse);
                    key.interestOps(SelectionKey.OP_WRITE);
                }else{
                    key.interestOps(SelectionKey.OP_READ);
                }
            } else {
                // more reading to be done
                key.interestOps(SelectionKey.OP_READ);
                selector.wakeup();
            }
        }

        /*
         * Process writes to ready sockets
         */
        private void write (SelectionKey key) throws Exception{
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

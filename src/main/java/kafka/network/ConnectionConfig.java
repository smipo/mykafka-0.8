package kafka.network;

public class ConnectionConfig {

    private String host;
    private int port;
    private int sendBufferSize = -1;
    private int receiveBufferSize = -1;
    private boolean tcpNoDelay = true;
    private boolean keepAlive = false;

    public String host() {
        return host;
    }

    public ConnectionConfig host(String host) {
        this.host = host;
        return this;
    }

    public int port() {
        return port;
    }

    public ConnectionConfig port(int port) {
        this.port = port;
        return this;
    }

    public int sendBufferSize() {
        return sendBufferSize;
    }

    public ConnectionConfig sendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public int receiveBufferSize() {
        return receiveBufferSize;
    }

    public ConnectionConfig receiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    public ConnectionConfig tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public boolean keepAlive() {
        return keepAlive;
    }

    public ConnectionConfig keepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }
}

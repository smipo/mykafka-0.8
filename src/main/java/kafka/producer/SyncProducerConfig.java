package kafka.producer;

import kafka.utils.Utils;

import java.util.Properties;

public class SyncProducerConfig {

    public Properties props;

    public SyncProducerConfig(Properties props){
        this.props = props;

        host = Utils.getString(props, "host");
        port = Utils.getInt(props, "port");

        bufferSize = Utils.getInt(props, "buffer.size", 100*1024);
        connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000);
        socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30000);
        reconnectInterval = Utils.getInt(props, "reconnect.interval", 30000);
        reconnectTimeInterval = Utils.getInt(props, "reconnect.time.interval.ms", 1000*1000*10);
        maxMessageSize = Utils.getInt(props, "max.message.size", 1000000);
    }

    /** the broker to which the producer sends events */
    public String host ;

    /** the port on which the broker is running */
    public int port ;

    public int bufferSize ;

    public int connectTimeoutMs ;

    /** the socket timeout for network requests */
    public int socketTimeoutMs ;

    public int reconnectInterval ;

    /** negative reconnect time interval means disabling this time-based reconnect feature */
    public int reconnectTimeInterval ;

    public int maxMessageSize;
}

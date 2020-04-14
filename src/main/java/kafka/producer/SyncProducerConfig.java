package kafka.producer;

import kafka.utils.VerifiableProperties;

import java.util.Properties;


public class SyncProducerConfig extends SyncProducerConfigShared{

    public static String DefaultClientId = "";
    public static short DefaultRequiredAcks  = 0;
    public static int DefaultAckTimeoutMs = 10000;


    public SyncProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        // no need to verify the property since SyncProducerConfig is supposed to be used internally
    }

    public SyncProducerConfig(VerifiableProperties props){
        super(props);

        host = props.getString("host");
        port = props.getInt("port");
        sendBufferBytes = props.getInt("send.buffer.bytes", 100*1024);
        clientId = props.getString("client.id", SyncProducerConfig.DefaultClientId);
        requestRequiredAcks = props.getShort("request.required.acks", SyncProducerConfig.DefaultRequiredAcks);
        requestTimeoutMs = props.getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs,
                1, Integer.MAX_VALUE);

    }

    /** the broker to which the producer sends events */
    public String host ;

    /** the port on which the broker is running */
    public int port ;

    public int sendBufferBytes ;

    /* the client application sending the producer requests */
    public String clientId ;

    /*
     * The required acks of the producer requests - negative value means ack
     * after the replicas in ISR have caught up to the leader's offset
     * corresponding to this produce request.
     */
    public short requestRequiredAcks;

    /*
     * The ack timeout of the producer requests. Value must be non-negative and non-zero
     */
    public int requestTimeoutMs ;


}

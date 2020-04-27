package kafka.cluster;

import kafka.api.ApiUtils;
import kafka.common.BrokerNotAvailableException;
import kafka.common.KafkaException;
import kafka.utils.JacksonUtils;
import kafka.utils.Utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class Broker {


    public  static  Broker createBroker(int id,String brokerInfoString) {
        if(brokerInfoString == null)
            throw new BrokerNotAvailableException(String.format("Broker id %s does not exist",id));
        try {
            if(brokerInfoString == null || brokerInfoString.isEmpty()){
                throw new BrokerNotAvailableException(String.format("Broker id %d does not exist",id));
            }else {
                Map<String,Object> brokerInfo = JacksonUtils.strToMap(brokerInfoString);
                String host = brokerInfo.get("host").toString();
                int port = Integer.parseInt(brokerInfo.get("port").toString());
                return new Broker(id, host, port);
            }
        } catch(Throwable t) {
            throw new KafkaException("Failed to parse the broker info from zookeeper: " + brokerInfoString, t);
        }
    }

    public static Broker readFrom(ByteBuffer buffer) throws UnsupportedEncodingException {
        int id = buffer.getInt();
        String host = ApiUtils.readShortString(buffer);
        int port = buffer.getInt();
        return new Broker(id, host, port);
    }

    protected int id;
    protected String host;
    protected int port;

    public Broker( int id,
                   String host,
                   int port){
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }


    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public void writeTo(ByteBuffer buffer) throws UnsupportedEncodingException {
        buffer.putInt(id);
        ApiUtils.writeShortString(buffer, host);
        buffer.putInt(port);
    }
    public int sizeInBytes() throws UnsupportedEncodingException{
        return ApiUtils.shortStringLength(host) /* host name */ + 4 /* port */ + 4; /* broker id*/
    }
    @Override
    public String toString(){
        return "id:" + id  + ",host:" + host + ",port:" + port;
    }

    public String  getConnectionString(){
        return host + ":" + port;
    }
    @Override
    public boolean equals(Object obj){
        if(obj == null ) return false;
        if(obj instanceof Broker){
            Broker n = (Broker)obj;
            return id == n.id && host == n.host && port == n.port;
        }
        return false;
    }
    @Override
    public int hashCode(){
        return Utils.hashcode(id, host, port);
    }
}

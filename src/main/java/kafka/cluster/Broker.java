package kafka.cluster;

import kafka.utils.Utils;

public class Broker {


    public static Broker createBroker(int id, String brokerInfoString) {
        String[] brokerInfo = brokerInfoString.split(":");
        return new Broker(id, brokerInfo[0], brokerInfo[1], Integer.parseInt(brokerInfo[2]));
    }

    protected int id;
    protected String creatorId;
    protected String host;
    protected int port;

    public Broker( int id,
                   String creatorId,
                   String host,
                   int port){
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String creatorId() {
        return creatorId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public String toString(){
        return "id:" + id + ",creatorId:" + creatorId + ",host:" + host + ",port:" + port;
    }

    public String  getZKString(){
        return creatorId + ":" + host + ":" + port;
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

package kafka.cluster;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class Cluster {

    private Map<Integer,Broker> brokers = new ConcurrentHashMap<>();

    public Cluster(){

    }
    public Cluster(List<Broker> brokerList) {
        for(Broker broker : brokerList)
            brokers.put(broker.id, broker);
    }

    public Broker getBroker( int id){
        return brokers.get(id);
    }

    public void add(Broker broker) {
        brokers.put(broker.id, broker);
    }

    public void remove(int id) {
        brokers.remove(id);
    }

    public int size() {
        return brokers.size();
    }

    @Override
    public String toString(){
        return "Cluster(" + brokers.values() + ")";
    }

}

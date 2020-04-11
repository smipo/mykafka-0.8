package kafka.common;

public class ClientIdAndBroker {

    public String clientId;
    public  String brokerInfo;

    public ClientIdAndBroker(String clientId, String brokerInfo) {
        this.clientId = clientId;
        this.brokerInfo = brokerInfo;
    }

    @Override
    public String toString() {
        return "ClientIdAndBroker{" +
                "clientId='" + clientId + '\'' +
                ", brokerInfo='" + brokerInfo + '\'' +
                '}';
    }
}

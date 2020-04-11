package kafka.common;

public class ClientIdAndTopic {

    public String clientId;
    public String topic;

    public ClientIdAndTopic(String clientId, String topic) {
        this.clientId = clientId;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "ClientIdAndTopic{" +
                "clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}

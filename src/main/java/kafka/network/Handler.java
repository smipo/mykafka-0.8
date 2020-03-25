package kafka.network;

public interface Handler {

    public Send handler(short requestTypeId, Receive request) throws Exception;
}

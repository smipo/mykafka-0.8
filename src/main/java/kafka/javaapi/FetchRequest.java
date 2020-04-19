package kafka.javaapi;

import kafka.api.RequestOrResponse;
import kafka.common.TopicAndPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class FetchRequest {

    int correlationId;
    String clientId;
    int maxWait;
    int minBytes;
    Map<TopicAndPartition, kafka.api.FetchRequest.PartitionFetchInfo> requestInfo;

    public FetchRequest(int correlationId, String clientId, int maxWait, int minBytes, Map<TopicAndPartition, kafka.api.FetchRequest.PartitionFetchInfo> requestInfo) {
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.requestInfo = requestInfo;
        underlying = new  kafka.api.FetchRequest(
                correlationId,
                kafka.api.FetchRequest.CurrentVersion,
                clientId,
                RequestOrResponse.OrdinaryConsumerId,
                maxWait,
                minBytes,
                requestInfo
        );
    }

    public  kafka.api.FetchRequest underlying;


    public void writeTo(ByteBuffer buffer) throws IOException { underlying.writeTo(buffer); }

    public int sizeInBytes() throws IOException {
        return underlying.sizeInBytes();
    }
}

package kafka.javaapi;

import kafka.api.RequestOrResponse;
import kafka.common.TopicAndPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class OffsetRequest {

    Map<TopicAndPartition, kafka.api.OffsetRequest.PartitionOffsetRequestInfo> requestInfo;
    short versionId;
    String clientId;

    public OffsetRequest(Map<TopicAndPartition, kafka.api.OffsetRequest.PartitionOffsetRequestInfo> requestInfo, short versionId, String clientId) {
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;
        underlying = new kafka.api.OffsetRequest(
                0,
                requestInfo ,
                versionId,
                clientId,
                RequestOrResponse.OrdinaryConsumerId
        );
    }

    public kafka.api.OffsetRequest underlying;

    public void writeTo(ByteBuffer buffer) throws IOException {
        underlying.writeTo(buffer) ;
    }


    public int sizeInBytes() throws IOException {
        return underlying.sizeInBytes();
    }

}

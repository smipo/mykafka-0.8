package kafka.api;

import kafka.cluster.Broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TopicMetadataResponse extends RequestOrResponse{

    List<TopicMetadata> topicsMetadata;

    public TopicMetadataResponse(int correlationId,List<TopicMetadata> topicsMetadata) {
        super(null,correlationId);
        this.topicsMetadata = topicsMetadata;
    }

    public int sizeInBytes() throws IOException {
        val brokers = extractBrokers(topicsMetadata).values
        4 + 4 + brokers.map(_.sizeInBytes).sum + 4 + topicsMetadata.map(_.sizeInBytes).sum
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        /* brokers */
        val brokers = extractBrokers(topicsMetadata).values
        buffer.putInt(brokers.size)
        brokers.foreach(_.writeTo(buffer))
        /* topic metadata */
        buffer.putInt(topicsMetadata.length)
        topicsMetadata.foreach(_.writeTo(buffer))
    }

    Map<Integer, Broker> extractBrokers(List<TopicMetadata> topicMetadatas) {
        val parts = topicsMetadata.flatMap(_.partitionsMetadata)
        val brokers = (parts.flatMap(_.replicas)) ++ (parts.map(_.leader).collect{case Some(l) => l})
        brokers.map(b => (b.id, b)).toMap
    }
}

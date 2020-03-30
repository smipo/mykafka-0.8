package kafka.api;

import kafka.cluster.Broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopicMetadataResponse extends RequestOrResponse{

    List<TopicMetadata> topicsMetadata;

    public TopicMetadataResponse(int correlationId,List<TopicMetadata> topicsMetadata) {
        super(null,correlationId);
        this.topicsMetadata = topicsMetadata;
    }

    public int sizeInBytes() throws IOException {
        Collection<Broker> brokers = extractBrokers(topicsMetadata).values();
        int sum = 0;
        for(Broker broker:brokers){
            sum += broker.sizeInBytes();
        }
        for(TopicMetadata metadata:topicsMetadata){
            sum += metadata.sizeInBytes();
        }
        return 4 + 4 + 4 + sum;
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        /* brokers */
        Collection<Broker> brokers = extractBrokers(topicsMetadata).values();
        buffer.putInt(brokers.size());
        for(Broker broker:brokers){
            broker.writeTo(buffer);
        }
        /* topic metadata */
        buffer.putInt(topicsMetadata.size());
        for(TopicMetadata metadata:topicsMetadata){
            metadata.writeTo(buffer);
        }
    }

    Map<Integer, Broker> extractBrokers(List<TopicMetadata> topicMetadatas) {
        List<TopicMetadata.PartitionMetadata> parts = topicsMetadata.stream()
                .map(topicMetadata -> topicMetadata.getPartitionsMetadata())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<Broker> brokers = parts.stream()
                .map(replica -> replica.getReplicas())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<Broker> leaderBrokers = parts.stream()
                .map(replica -> replica.getLeader())
                .collect(Collectors.toList());
        brokers.addAll(leaderBrokers);
        return brokers.stream().collect(Collectors.toMap(Broker::id, broker -> broker));
    }
}
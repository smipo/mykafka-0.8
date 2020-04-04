package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetResponse extends RequestOrResponse{

    public static OffsetResponse readFrom(ByteBuffer buffer) throws IOException{
        int correlationId = buffer.getInt();
        int numTopics = buffer.getInt();
        Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets = new HashMap<>();
        for(int i = 1;i < numTopics;i++){
            String topic = ApiUtils.readShortString(buffer);
            int numPartitions = buffer.getInt();
            for(int j = 1;j < numPartitions;i++){
                int partition = buffer.getInt();
                short error = buffer.getShort();
                int numOffsets = buffer.getInt();
                List<Long> offsets = new ArrayList<>();
                for(int k = 1;k < numOffsets;k++){
                    offsets.add(buffer.getLong());
                }
                partitionErrorAndOffsets.put(new TopicAndPartition(topic, partition),new PartitionOffsetsResponse(error, offsets));
            }
        }
        return new OffsetResponse(correlationId, partitionErrorAndOffsets);
    }
    public static  class PartitionOffsetsResponse{
        public short error;
        public List<Long> offsets;

        public PartitionOffsetsResponse(short error, List<Long> offsets) {
            this.error = error;
            this.offsets = offsets;
        }
    }

    Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets;

    public OffsetResponse(int correlationId,Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets) {
        super(null, correlationId);
        this.partitionErrorAndOffsets = partitionErrorAndOffsets;

        for (Map.Entry<TopicAndPartition, PartitionOffsetsResponse> entry : partitionErrorAndOffsets.entrySet()) {
            List<Pair<TopicAndPartition, PartitionOffsetsResponse>> list = offsetsGroupedByTopic.get(entry.getKey().topic());
            if(list == null){
                list = new ArrayList<>();
                offsetsGroupedByTopic.put(entry.getKey().topic(),list);
            }
            list.add(new Pair<>(entry.getKey(),entry.getValue()));
        }
    }
    Map<String, List<Pair<TopicAndPartition, PartitionOffsetsResponse>>> offsetsGroupedByTopic = new HashMap<>();

    public  boolean hasError(){
        return partitionErrorAndOffsets.values().stream().allMatch(p -> p.error != ErrorMapping.NoError);
    }


    public int sizeInBytes() throws IOException {
        int size =  4 + /* correlation id */
                4 ; /* topic count */
        for (Map.Entry<String, List<Pair<TopicAndPartition, PartitionOffsetsResponse>>> entry : offsetsGroupedByTopic.entrySet()) {
            size += ApiUtils.shortStringLength(entry.getKey());
            for(Pair<TopicAndPartition, PartitionOffsetsResponse> pair:entry.getValue()){
                size += 4 + /* partition id */
                        2 + /* partition error */
                        4 + /* offset array length */
                        pair.getValue().offsets.size() * 8; /* offset */
            }
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        buffer.putInt(offsetsGroupedByTopic.size());// topic count
        for (Map.Entry<String, List<Pair<TopicAndPartition, PartitionOffsetsResponse>>> entry : offsetsGroupedByTopic.entrySet()) {
            String topic = entry.getKey();
            ApiUtils.writeShortString(buffer, topic);
            List<Pair<TopicAndPartition, PartitionOffsetsResponse>> list = entry.getValue();
            buffer.putInt(list.size()); // partition count
            for(Pair<TopicAndPartition, PartitionOffsetsResponse> pair:entry.getValue()){
                buffer.putInt(pair.getKey().partition());
                buffer.putShort(pair.getValue().error);
                buffer.putInt(pair.getValue().offsets.size()) ;// offset array length
                pair.getValue().offsets.forEach(offset->buffer.putLong(offset));
            }
        }
    }
}

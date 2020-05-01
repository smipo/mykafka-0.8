package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProducerResponse  extends RequestOrResponse{

    public static ProducerResponse readFrom(ByteBuffer buffer) throws IOException{
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, ProducerResponseStatus> status = new HashMap<>();
        for (int i = 1;i < topicCount;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partitionCount = buffer.getInt();
            for (int j = 1;j < partitionCount;j++){
                int partition = buffer.getInt();
                short error = buffer.getShort();
                long offset = buffer.getLong();
                status.put(new TopicAndPartition(topic, partition), new ProducerResponseStatus(error, offset));
            }
        }
        return new ProducerResponse(correlationId, status);
    }

    public static class ProducerResponseStatus{
        public short error;
        public long offset;

        public ProducerResponseStatus(short error, long offset) {
            this.error = error;
            this.offset = offset;
        }
    }

    public Map<TopicAndPartition, ProducerResponseStatus> status;

    public ProducerResponse(int correlationId,Map<TopicAndPartition, ProducerResponseStatus> status) {
        super(null,correlationId);
        this.status = status;

        for (Map.Entry<TopicAndPartition, ProducerResponseStatus> entry : status.entrySet()) {
            List<Pair<TopicAndPartition, ProducerResponseStatus>> list = statusGroupedByTopic.get(entry.getKey().topic());
            if(list == null){
                list = new ArrayList<>();
                statusGroupedByTopic.put(entry.getKey().topic(),list);
            }
            list.add(new Pair<>(entry.getKey(),entry.getValue()));
        }
    }

    Map<String, List<Pair<TopicAndPartition, ProducerResponseStatus>>> statusGroupedByTopic = new HashMap<>();

    public int sizeInBytes() throws IOException {
        int size =  4 + /* correlation id */
                4 ;/* topic count */
        Map<String, List<Pair<TopicAndPartition, ProducerResponseStatus>>> groupedStatus = statusGroupedByTopic;
        for (Map.Entry<String,  List<Pair<TopicAndPartition, ProducerResponseStatus>>> entry : groupedStatus.entrySet()) {
            size += ApiUtils.shortStringLength(entry.getKey())+
            4 + /* partition count for this topic */
                    + entry.getValue().size()*(
                    4 + /* partition id */
                            2 + /* error code */
                            8); /* offset */
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        Map<String, List<Pair<TopicAndPartition, ProducerResponseStatus>>> groupedStatus = statusGroupedByTopic;
        buffer.putInt(correlationId);
        buffer.putInt(groupedStatus.size()); // topic count
        for (Map.Entry<String,  List<Pair<TopicAndPartition, ProducerResponseStatus>>> entry : groupedStatus.entrySet()) {
            ApiUtils.writeShortString(buffer, entry.getKey());
            buffer.putInt(entry.getValue().size()); // partition count
            for(Pair<TopicAndPartition, ProducerResponseStatus> pair:entry.getValue()){
                buffer.putInt(pair.getKey().partition());
                buffer.putShort(pair.getValue().error);
                buffer.putLong(pair.getValue().offset);
            }
        }
    }
    public boolean hasError(){
        return status.values().stream().allMatch(p -> p.error != ErrorMapping.NoError);
    }
}

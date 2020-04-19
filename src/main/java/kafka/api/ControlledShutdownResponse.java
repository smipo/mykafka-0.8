package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class ControlledShutdownResponse extends RequestOrResponse{

    public static ControlledShutdownResponse readFrom(ByteBuffer buffer) throws IOException{
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();

        Set<TopicAndPartition> partitionsRemaining = new HashSet<>();
        for (int i = 0;i < numEntries;i++ ){
            String topic = ApiUtils.readShortString(buffer);
            int partition = buffer.getInt();
            partitionsRemaining .add(new TopicAndPartition(topic, partition));
        }
       return new ControlledShutdownResponse(correlationId, errorCode, partitionsRemaining);
    }

    public short errorCode = ErrorMapping.NoError;
    public Set<TopicAndPartition> partitionsRemaining;

    public ControlledShutdownResponse(int correlationId, short errorCode, Set<TopicAndPartition> partitionsRemaining) {
        super(null,correlationId);
        this.errorCode = errorCode;
        this.partitionsRemaining = partitionsRemaining;
    }

    public  int sizeInBytes() throws IOException {
        int size =  4 /* correlation id */ +
                2 /* error code */ +
                4 /* number of responses */;
        for (TopicAndPartition topicAndPartition : partitionsRemaining) {
            size +=
                    2 + topicAndPartition.topic().length() /* topic */ +
                            4; /* partition */
        }
        return size;
    }

    public  void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(partitionsRemaining.size());
        for (TopicAndPartition topicAndPartition : partitionsRemaining){
            ApiUtils.writeShortString(buffer, topicAndPartition.topic());
            buffer.putInt(topicAndPartition.partition());
        }
    }
}

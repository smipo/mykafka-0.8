package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;
import kafka.network.MultiSend;
import kafka.network.Send;
import kafka.utils.Pair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.*;

public class FetchResponse {

    public static int headerSize() {
        return 4 + /* correlationId */
                4; /* topic count */
    }
    public static  FetchResponse readFrom(ByteBuffer buffer)  throws UnsupportedEncodingException{
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, FetchResponsePartitionData> data = new HashMap<>();
        for(int i = 0;i < topicCount;i++){
            TopicData topicData = TopicData.readFrom(buffer);
            for(Map.Entry<Integer, FetchResponsePartitionData> entry : topicData.partitionData.entrySet()){
                TopicAndPartition topicAndPartition = new TopicAndPartition(topicData.topic,entry.getKey());
                data.put(topicAndPartition,entry.getValue());
            }
        }
        return new FetchResponse(correlationId, data);
    }

    public int correlationId;
    public Map<TopicAndPartition, FetchResponsePartitionData> data;

    public FetchResponse(int correlationId, Map<TopicAndPartition, FetchResponsePartitionData> data)  throws UnsupportedEncodingException{
        this.correlationId = correlationId;
        this.data = data;
        for(Map.Entry<TopicAndPartition, FetchResponsePartitionData> entry : data.entrySet()){
            TopicAndPartition topicAndPartition = entry.getKey();
            FetchResponsePartitionData fetchResponsePartitionData = entry.getValue();
            List<Pair<TopicAndPartition, FetchResponsePartitionData>> list = dataGroupedByTopic.get(topicAndPartition.topic());
            if(list == null){
                list = new ArrayList<>();
                dataGroupedByTopic.put(topicAndPartition.topic(),list);
            }
            list.add(new Pair<>(topicAndPartition,fetchResponsePartitionData));
        }
        sizeInBytes = headerSize();
        for(Map.Entry<String, List<Pair<TopicAndPartition, FetchResponsePartitionData>>> entry : dataGroupedByTopic.entrySet()){
            String topic = entry.getKey();
            Map<Integer, FetchResponsePartitionData> partitionData = new HashMap<>();
            for(Pair<TopicAndPartition, FetchResponsePartitionData> pair:entry.getValue()){
                partitionData.put(pair.getKey().partition(),pair.getValue());
            }
            TopicData topicData = new TopicData(topic,partitionData);
            sizeInBytes += topicData.sizeInBytes;
        }
    }

    public Map<String, List<Pair<TopicAndPartition, FetchResponsePartitionData>>> dataGroupedByTopic = new HashMap<>();

    public int sizeInBytes;

    private FetchResponsePartitionData partitionDataFor(String topic, int partition){
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        FetchResponsePartitionData fetchResponsePartitionData = data.get(topicAndPartition);
        if(fetchResponsePartitionData != null){
            return fetchResponsePartitionData;
        }else{
            throw new IllegalArgumentException(
                    String.format("No partition %s in fetch response %s",topicAndPartition + "", this.toString()));
        }
    }

    public ByteBufferMessageSet messageSet(String topic, int partition) {
        return (ByteBufferMessageSet)partitionDataFor(topic, partition).messages;
    }


    public long highWatermark(String topic, int partition) {
        return partitionDataFor(topic, partition).hw;
    }

    public boolean hasError (){
        return data.values().contains(ErrorMapping.NoError);
    }

    public short errorCode(String topic, int partition) {
        return partitionDataFor(topic, partition).error;
    }

    public static class FetchResponsePartitionData{

        public static FetchResponsePartitionData readFrom(ByteBuffer buffer) {
            short error = buffer.getShort();
            long hw = buffer.getLong();
            int messageSetSize = buffer.getInt();
            ByteBuffer messageSetBuffer = buffer.slice();
            messageSetBuffer.limit(messageSetSize);
            buffer.position(buffer.position() + messageSetSize);
            return new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer));
        }

        public static int headerSize =
                2 + /* error code */
                        8 + /* high watermark */
                        4 ;/* messageSetSize */

        public short error = ErrorMapping.NoError;
        public  long hw = -1L;
        public MessageSet messages;

        public FetchResponsePartitionData(short error, long hw, MessageSet messages) {
            this.error = error;
            this.hw = hw;
            this.messages = messages;
        }

        public int sizeInBytes(){
            return FetchResponsePartitionData.headerSize + messages.sizeInBytes();
        }
    }

    public static class PartitionDataSend extends Send {
        public int partitionId;
        public FetchResponsePartitionData partitionData;

        public PartitionDataSend(int partitionId, FetchResponsePartitionData partitionData) {
            this.partitionId = partitionId;
            this.partitionData = partitionData;

            messageSize = partitionData.messages.sizeInBytes();
            buffer = ByteBuffer.allocate( 4 /** partitionId **/ + FetchResponsePartitionData.headerSize);
            buffer.putInt(partitionId);
            buffer.putShort(partitionData.error);
            buffer.putLong(partitionData.hw);
            buffer.putInt(partitionData.messages.sizeInBytes());
            buffer.rewind();
        }
        private int messageSize ;
        private int messagesSentSize = 0;
        private ByteBuffer buffer;

        public  boolean complete() {
            return !buffer.hasRemaining() && messagesSentSize >= messageSize;
        }

        public long writeTo(GatheringByteChannel channel) throws IOException{
            int written = 0;
            if(buffer.hasRemaining())
                written += channel.write(buffer);
            if(!buffer.hasRemaining() && messagesSentSize < messageSize) {
                long bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize);
                messagesSentSize += bytesSent;
                written += bytesSent;
            }
            return written;
        }
    }
    public static class TopicData{

        public static  TopicData readFrom(ByteBuffer buffer) throws UnsupportedEncodingException{
            String topic = ApiUtils.readShortString(buffer);
            int partitionCount = buffer.getInt();
            Map<Integer, FetchResponsePartitionData> topicPartitionDataPairs = new HashMap<>();
            for(int i = 0;i < partitionCount;i++){
                int partitionId = buffer.getInt();
                FetchResponsePartitionData partitionData = FetchResponsePartitionData.readFrom(buffer);
                topicPartitionDataPairs.put(partitionId, partitionData);
            }
            return new TopicData(topic, topicPartitionDataPairs);
        }

        public static int headerSize(String topic)  throws UnsupportedEncodingException {
            return ApiUtils.shortStringLength(topic) +
                    4; /* partition count */
        }
        public String topic;
        public Map<Integer, FetchResponsePartitionData> partitionData;

        public TopicData(String topic, Map<Integer, FetchResponsePartitionData> partitionData) throws UnsupportedEncodingException{
            this.topic = topic;
            this.partitionData = partitionData;

            sizeInBytes = TopicData.headerSize(topic) +
                    partitionData.values().stream().mapToInt(FetchResponsePartitionData::sizeInBytes).sum();
            headerSize = TopicData.headerSize(topic);
        }
        public int sizeInBytes;
        public int headerSize;
    }

    public static class TopicDataSend extends Send{
        public TopicData topicData;

        public TopicDataSend(TopicData topicData) throws UnsupportedEncodingException{
            this.topicData = topicData;
            size = topicData.sizeInBytes;

            buffer = ByteBuffer.allocate(topicData.headerSize);
            ApiUtils.writeShortString(buffer, topicData.topic);
            buffer.putInt(topicData.partitionData.size());
            buffer.rewind();
            List<Send> finalSends = new ArrayList<>();
            topicData.partitionData.forEach((k, v) ->
                    finalSends.add(new PartitionDataSend(k,v))
            );
            this.sends = new MultiSend<Send>(finalSends) {
            };
            sends.expectedBytesToWrite = topicData.sizeInBytes - topicData.headerSize;
        }

        private int size ;

        private int sent = 0;

        private ByteBuffer buffer ;

        public MultiSend<Send> sends ;

        public  boolean complete() {
            return sent >= size;
        }

        public long writeTo(GatheringByteChannel channel) throws IOException{
            expectIncomplete();
            int written = 0;
            if(buffer.hasRemaining())
                written += channel.write(buffer);
            if(!buffer.hasRemaining() && !sends.complete()) {
                written += sends.writeTo(channel);
            }
            sent += written;
            return written;
        }
    }

    public static  class FetchResponseSend extends Send{
       public FetchResponse fetchResponse;

        public FetchResponseSend(FetchResponse fetchResponse) throws UnsupportedEncodingException{
            this.fetchResponse = fetchResponse;

            size = fetchResponse.sizeInBytes;
            sendSize = 4 /* for size */ + size;
            buffer = ByteBuffer.allocate(4 /* for size */ + FetchResponse.headerSize());
            buffer.putInt(size);
            buffer.putInt(fetchResponse.correlationId);
            buffer.putInt(fetchResponse.dataGroupedByTopic.size()); // topic count
            buffer.rewind();

            List<Send> sendList = new ArrayList<>();
            for(Map.Entry<String, List<Pair<TopicAndPartition, FetchResponsePartitionData>>> entry : fetchResponse.dataGroupedByTopic.entrySet()){
                Map<Integer, FetchResponsePartitionData> partitionData = new HashMap<>();
                for(Pair<TopicAndPartition, FetchResponsePartitionData> pair:entry.getValue()){
                    partitionData.put(pair.getKey().partition(),pair.getValue());
                }
                TopicData topicData = new TopicData(entry.getKey(),partitionData);
                TopicDataSend topicDataSend = new TopicDataSend(topicData);
                sendList.add(topicDataSend);
            }
            sends = new MultiSend(sendList){

            };
            sends.expectedBytesToWrite = fetchResponse.sizeInBytes - FetchResponse.headerSize();
        }

        private int size ;

        private int sent = 0;

        private int sendSize ;

        public  boolean complete() {
            return sent >= sendSize;
        }

        private ByteBuffer buffer ;


        MultiSend<Send> sends ;

        public long writeTo(GatheringByteChannel channel) throws IOException{
            expectIncomplete();
            int written = 0;
            if(buffer.hasRemaining())
                written += channel.write(buffer);
            if(!buffer.hasRemaining() && !sends.complete()) {
                written += sends.writeTo(channel);
            }
            sent += written;
            return written;
        }
    }
}

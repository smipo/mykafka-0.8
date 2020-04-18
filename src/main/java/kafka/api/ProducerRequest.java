package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProducerRequest   extends RequestOrResponse{

    public static short CurrentVersion = 0;

    public static ProducerRequest readFrom(ByteBuffer buffer) throws IOException{
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId  = ApiUtils.readShortString(buffer);
        short requiredAcks = buffer.getShort();
        int ackTimeoutMs = buffer.getInt();
        //build the topic structure
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, ByteBufferMessageSet> data = new HashMap<>();
        for(int i = 0;i < topicCount;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partitionCount = buffer.getInt();
            for(int j = 1;j < partitionCount;j++){
                int partition = buffer.getInt();
                int messageSetSize = buffer.getInt();
                byte[] messageSetBuffer = new byte[messageSetSize];
                buffer.get(messageSetBuffer,0,messageSetSize);
                data.put(new TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)));
            }
        }
       return new ProducerRequest(correlationId,versionId, clientId, requiredAcks, ackTimeoutMs, data);
    }
    public  short versionId;
    public  String clientId;
    public short requiredAcks;
    public int ackTimeoutMs;
    public  Map<TopicAndPartition, ByteBufferMessageSet> data;

    public ProducerRequest( int correlationId,short versionId, String clientId, short requiredAcks, int ackTimeoutMs, Map<TopicAndPartition, ByteBufferMessageSet> data) {
        super(RequestKeys.ProduceKey,correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.requiredAcks = requiredAcks;
        this.ackTimeoutMs = ackTimeoutMs;
        this.data = data;

        for (Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : data.entrySet()) {
            List<Pair<TopicAndPartition, ByteBufferMessageSet>> list = dataGroupedByTopic.get(entry.getKey().topic());
            if(list == null){
                list = new ArrayList<>();
                dataGroupedByTopic.put(entry.getKey().topic(),list);
            }
            list.add(new Pair<>(entry.getKey(),entry.getValue()));
            topicPartitionMessageSizeMap.put(entry.getKey(),entry.getValue().sizeInBytes());
        }
    }

    public ProducerRequest(int correlationId,
                           String clientId,
                           short requiredAcks,
                           int ackTimeoutMs,
                           Map<TopicAndPartition, ByteBufferMessageSet> data) {
        this(correlationId,ProducerRequest.CurrentVersion, clientId, requiredAcks, ackTimeoutMs, data);
    }

    public Map<String, List<Pair<TopicAndPartition, ByteBufferMessageSet>>> dataGroupedByTopic = new HashMap<>();
    public Map<TopicAndPartition,Integer> topicPartitionMessageSizeMap = new HashMap<>();

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putShort(requiredAcks);
        buffer.putInt(ackTimeoutMs);

        //save the topic structure
        buffer.putInt(dataGroupedByTopic.size()) ;//the number of topics
        for (Map.Entry<String, List<Pair<TopicAndPartition, ByteBufferMessageSet>>> entry : dataGroupedByTopic.entrySet()) {
            ApiUtils.writeShortString(buffer, entry.getKey()); //write the topic
            buffer.putInt(entry.getValue().size()); //the number of partitions
            for(Pair<TopicAndPartition, ByteBufferMessageSet> pair:entry.getValue()){
                buffer.putInt(pair.getKey().partition());
                buffer.putInt(pair.getValue().buffer().limit());
                buffer.put(pair.getValue().buffer());
                pair.getValue().buffer().rewind();
            }
        }
    }

    public int sizeInBytes() throws IOException {
        int size =
        2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.shortStringLength(clientId) + /* client id */
                2 + /* requiredAcks */
                4 + /* ackTimeoutMs */
                4 ; /* number of topics */
        for (Map.Entry<String, List<Pair<TopicAndPartition, ByteBufferMessageSet>>> entry : dataGroupedByTopic.entrySet()) {
            size +=  ApiUtils.shortStringLength(entry.getKey()) +
                    4 ; /* the number of partitions */
            for(Pair<TopicAndPartition, ByteBufferMessageSet> pair:entry.getValue()){
                size +=  4 + /* partition id */
                        4 + /* byte-length of serialized messages */
                        pair.getValue().sizeInBytes();
            }
        }
        return size;
    }

    public int numPartitions(){
        return data.size();
    }

    public  void emptyData(){
        data.clear();
    }
    public  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request)throws IOException,InterruptedException{
        if(request.requestObj instanceof ProducerRequest) {
            ProducerRequest p = (ProducerRequest)request.requestObj;
            if(p.requiredAcks == 0){
                requestChannel.closeConnection(request.processor, request);
                return;
            }
        }
        Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus> status = new HashMap<>();
        for (Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : data.entrySet()) {
            status.put(entry.getKey(),new ProducerResponse.ProducerResponseStatus(ErrorMapping.codeFor(e.getClass().getName()), -1l));
        }
        ProducerResponse errorResponse = new ProducerResponse(correlationId, status);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}

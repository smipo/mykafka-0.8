package kafka.api;

import kafka.common.KafkaException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RequestKeys {

    public static short ProduceKey = 0;
    public static short FetchKey = 1;
    public static short OffsetsKey = 2;
    public static short MetadataKey = 3;
    public static short LeaderAndIsrKey = 4;
    public static short StopReplicaKey = 5;
    public static short UpdateMetadataKey = 6;
    public static short ControlledShutdownKey = 7;

    public static RequestOrResponse keyToNameAndDeserializerMap(short key, ByteBuffer buffer) throws IOException {
        if(key == ProduceKey){
            return ProducerRequest.readFrom(buffer);
        }else if(key == FetchKey){
            return FetchRequest.readFrom(buffer);
        }else if(key == OffsetsKey){
            return OffsetRequest.readFrom(buffer);
        }else if(key == MetadataKey){
            return  TopicMetadataRequest.readFrom(buffer);
        }else if(key == LeaderAndIsrKey){
            return LeaderAndIsrRequest.readFrom(buffer);
        }else if(key == StopReplicaKey){
            return StopReplicaRequest.readFrom(buffer);
        }else if(key == UpdateMetadataKey){
            return UpdateMetadataRequest.readFrom(buffer);
        }else if(key == ControlledShutdownKey){
            return ControlledShutdownRequest.readFrom(buffer);
        }
        return null;
    }

    public static String keyToNameAndDeserializerMap(short key) {
        if(key == ProduceKey){
            return "Produce";
        }else if(key == FetchKey){
            return "Fetch";
        }else if(key == OffsetsKey){
            return "Offsets";
        }else if(key == MetadataKey){
            return "Metadata";
        }else if(key == LeaderAndIsrKey){
            return "LeaderAndIsr";
        }else if(key == StopReplicaKey){
            return "StopReplica";
        }else if(key == UpdateMetadataKey){
            return "UpdateMetadata";
        }else if(key == ControlledShutdownKey){
            return "ControlledShutdown";
        }
        return null;
    }

    public static String nameForKey(short key) {
        String name = keyToNameAndDeserializerMap(key);
        if(name == null){
            throw new KafkaException(String.format("Wrong request type %d",key ));
        }
        return name;
    }

    public static RequestOrResponse deserializerForKey(short key,ByteBuffer buffer) throws IOException{
        RequestOrResponse response = keyToNameAndDeserializerMap(key,buffer);
        if(response == null){
            throw new KafkaException(String.format("Wrong request type %d",key ));
        }
        return  response;
    }
}

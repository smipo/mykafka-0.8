package kafka.javaapi;

public class FetchResponse {

     kafka.api.FetchResponse underlying;

    public FetchResponse(kafka.api.FetchResponse underlying) {
        this.underlying = underlying;
    }

    public kafka.javaapi.message.ByteBufferMessageSet messageSet(String topic,int  partition){
        return new kafka.javaapi.message.ByteBufferMessageSet(underlying.messageSet(topic, partition).buffer());
    }

    public long highWatermark(String topic,int partition) {
        return underlying.highWatermark(topic, partition);
    }
    public boolean hasError(){
        return underlying.hasError();
    }

    public int errorCode(String topic,int partition) {
        return underlying.errorCode(topic, partition);
    }
}

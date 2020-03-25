package kafka.api;

import kafka.network.Request;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MultiProducerRequest extends Request {

    ProducerRequest[] produces;

    public MultiProducerRequest(ProducerRequest[] produces){
        super(RequestKeys.MultiProduce);
        this.produces = produces;
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer)throws IOException{
        short count = buffer.getShort();
        ProducerRequest[] produces = new  ProducerRequest[count];
        for(int i = 0;i < count;i++){
            produces[i] = ProducerRequest.readFrom(buffer);
        }
        return new MultiProducerRequest(produces);
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        if(produces.length > Short.MAX_VALUE)
            throw new IllegalArgumentException("Number of requests in MultiProducer exceeds " + Short.MAX_VALUE + ".");
        buffer.putShort((short) produces.length);
        for(ProducerRequest produce : produces)
            produce.writeTo(buffer);
    }

    public int sizeInBytes(){
        int size = 2;
        for(ProducerRequest produce : produces)
            size += produce.sizeInBytes();
        return size;
    }

    public String toString(){
        StringBuffer buffer = new StringBuffer();
        for(ProducerRequest produce : produces) {
            buffer.append(produce.toString());
            buffer.append(",");
        }
        return buffer.toString();
    }

    public ProducerRequest[] produces() {
        return produces;
    }
}

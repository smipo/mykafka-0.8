package kafka.javaapi.producer;

import kafka.producer.SyncProducerConfig;
import kafka.javaapi.message.ByteBufferMessageSet;

public class SyncProducer {

    kafka.producer.SyncProducer syncProducer;

    public SyncProducer(kafka.producer.SyncProducer syncProducer) {
        this.syncProducer = syncProducer;
        underlying = syncProducer;
    }

    public SyncProducer(SyncProducerConfig config) {
        this(new kafka.producer.SyncProducer(config));
    }

    kafka.producer.SyncProducer underlying ;

    public void send(String topic, int partition, ByteBufferMessageSet messages)throws Exception {
        underlying.send(topic, partition, messages.underlying());
    }

    public void send(String topic, ByteBufferMessageSet messages)throws Exception{
        send(topic,
                kafka.api.ProducerRequest.RandomPartition,
                messages);
    }

    public void multiSend(kafka.javaapi.ProducerRequest... produces) throws Exception{
        kafka.api.ProducerRequest[] produceRequests = new kafka.api.ProducerRequest[produces.length];
        for(int i = 0 ;i < produces.length;i++)
            produceRequests[i] = new kafka.api.ProducerRequest(produces[i].topic(), produces[i].partition(), produces[i].messages().underlying());
        underlying.multiSend(produceRequests);
    }

    public void close() {
        underlying.close();
    }

}

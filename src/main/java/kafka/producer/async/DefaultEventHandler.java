package kafka.producer.async;

import kafka.api.ProducerRequest;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.NoCompressionCodec;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducer;
import kafka.serializer.Encoder;
import kafka.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class DefaultEventHandler<T> implements  EventHandler<T>{

    private static Logger logger = Logger.getLogger(DefaultEventHandler.class);

    ProducerConfig config;
    CallbackHandler<T> cbkHandler;

    public DefaultEventHandler(ProducerConfig config, CallbackHandler<T> cbkHandler) {
        this.config = config;
        this.cbkHandler = cbkHandler;
    }

    public void init(Properties props) { }

    public void handle(QueueItem<T>[] events, SyncProducer syncProducer, Encoder<T> serializer)  throws Exception{
        QueueItem<T>[] processedEvents = events;
        if(cbkHandler != null) {
            List<QueueItem<T>> queueItemList = cbkHandler.beforeSendingData(events);
            QueueItem<T>[] queueItems = new QueueItem[queueItemList.size()];
            processedEvents = queueItemList.toArray(queueItems);
        }

        send(serialize(collate(processedEvents), serializer), syncProducer);
    }

    private void send(Map<Pair<String, Integer>, ByteBufferMessageSet> messagesPerTopic, SyncProducer syncProducer) throws Exception{
        if(messagesPerTopic.size() > 0) {
            ProducerRequest[] requests = new ProducerRequest[messagesPerTopic.size()];
            int index = 0;
            for (Map.Entry<Pair<String, Integer>, ByteBufferMessageSet> entry : messagesPerTopic.entrySet()) {
                Pair<String, Integer> key = entry.getKey();
                requests[index] =  new ProducerRequest(key.getKey(),key.getValue(),entry.getValue());
                index++;
            }
            int maxAttempts = config.numRetries + 1;
            int attemptsRemaining = maxAttempts;
            boolean sent = false;

            while (attemptsRemaining > 0 && !sent) {
                attemptsRemaining -= 1;
                try {
                    syncProducer.multiSend(requests);
                    logger.trace("kafka producer sent messages for topics %s to broker %s:%d (on attempt %d)"
                            .format(messagesPerTopic + "", syncProducer.config().host, syncProducer.config().port, maxAttempts - attemptsRemaining));
                    sent = true;
                }
                catch (Exception e){
                    logger.warn("Error sending messages, %d attempts remaining".format(String.valueOf(attemptsRemaining)), e);
                    if (attemptsRemaining == 0)
                        throw e;
                }
            }
        }
    }

    private Map<Pair<String, Integer>, ByteBufferMessageSet> serialize(Map<Pair<String,Integer>, T> eventsPerTopic, Encoder<T> serializer) throws IOException {
        Map<Pair<String, Integer>, Message> eventsPerTopicMap = new HashMap<>();
        for (Map.Entry<Pair<String,Integer>, T> entry : eventsPerTopic.entrySet()) {
            eventsPerTopicMap.put(entry.getKey(),serializer.toMessage(entry.getValue()));
        }
        /** enforce the compressed.topics config here.
         *  If the compression codec is anything other than NoCompressionCodec,
         *    Enable compression only for specified topics if any
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */
        Map<Pair<String, Integer>, ByteBufferMessageSet> messagesPerTopicPartition = new HashMap<>();
        for (Map.Entry<Pair<String,Integer>, T> entry : eventsPerTopic.entrySet()) {
            Pair<String,Integer> p = entry.getKey();
            Message message = serializer.toMessage(entry.getValue());
            if(config.compressionCodec instanceof NoCompressionCodec){
                logger.trace("Sending %d messages with no compression to topic %s on partition %d"
                        .format(message.size() + "", p.getKey(), p.getValue()));
                messagesPerTopicPartition.put(p,new ByteBufferMessageSet(new NoCompressionCodec(), message));
            }else{
                if(config.compressedTopics.size() == 0 || config.compressedTopics.contains(p.getKey())){
                    logger.trace("Sending %d messages with compression codec %d to topic %s on partition %d"
                            .format(message.size() + "", config.compressionCodec.codec(), p.getKey(), p.getValue()));
                    messagesPerTopicPartition.put(p,new ByteBufferMessageSet(config.compressionCodec, message));
                }else{
                    logger.trace("Sending %d messages to topic %s and partition %d with no compression as %s is not in compressed.topics - %s"
                            .format(message.size() + "",  p.getKey(), p.getValue(), p.getKey(),
                                    config.compressedTopics.toString()));
                    messagesPerTopicPartition.put(p,new ByteBufferMessageSet(new NoCompressionCodec(), message));
                }
            }
        }
        return messagesPerTopicPartition;
    }

    private Map<Pair<String,Integer>, T> collate(QueueItem<T>[] events){
        Map<Pair<String,Integer>, T> collatedEvents = new HashMap<>();
        Set<String> distinctTopics = new HashSet<>();
        Set<Integer> distinctPartitions = new HashSet<>();
        for(QueueItem<T> item:events){
            distinctTopics.add(item.getTopic());
            distinctPartitions.add(item.getPartition());
        }
        Set<QueueItem<T>> remainingEvents = new HashSet<>();
        for(QueueItem<T> event:events){
            remainingEvents.add(event);
        }
        List<QueueItem<T>> conformEvents = new ArrayList<>();
        for(String topic:distinctTopics){
            Iterator<QueueItem<T>> it = remainingEvents.iterator();
            while (it.hasNext()) {
                QueueItem<T> item = it.next();
                if(item.getTopic().equals(topic)){
                    conformEvents.add(item);
                    it.remove();
                }
            }
            for(Integer p:distinctPartitions){
                for(QueueItem<T> event:conformEvents){
                    if(p == event.partition){
                        collatedEvents.put(new Pair<>(topic, p),event.data);
                    }
                }
            }
            conformEvents.clear();
        }
        return collatedEvents;
    }

    public void close() {
    }
}

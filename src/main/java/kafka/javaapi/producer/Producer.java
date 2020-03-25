package kafka.javaapi.producer;

import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.producer.SyncProducer;
import kafka.producer.async.QueueItem;
import kafka.serializer.Encoder;
import kafka.utils.Utils;
import org.omg.CORBA.Object;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Producer<K,V> {

    ProducerConfig config;
    Partitioner<K> partitioner;
    ProducerPool<V> producerPool;
    boolean populateProducerPool = true;

    public Producer(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool, boolean populateProducerPool) {
       init(config, partitioner,  producerPool,  populateProducerPool);
    }
    /**
     * This constructor can be used when all config parameters will be specified through the
     * ProducerConfig object
     * @param config Producer Configuration object
     */
    public Producer(ProducerConfig config) throws ClassNotFoundException,InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        init(config, Utils.getObject(config.partitionerClass) == null?null:(Partitioner<K>)Utils.getObject(config.partitionerClass),
                new ProducerPool<V>(config, Utils.getObject(config.serializerClass) == null?null: (Encoder<V>)Utils.getObject(config.serializerClass) ),
                true);
    }

    /**
     * This constructor can be used to provide pre-instantiated objects for all config parameters
     * that would otherwise be instantiated via reflection. i.e. encoder, partitioner, event handler and
     * callback handler
     * @param config Producer Configuration object
     * @param encoder Encoder used to convert an object of type V to a kafka.message.Message
     * @param eventHandler the class that implements kafka.javaapi.producer.async.IEventHandler[T] used to
     * dispatch a batch of produce requests, using an instance of kafka.javaapi.producer.SyncProducer
     * @param cbkHandler the class that implements kafka.javaapi.producer.async.CallbackHandler[T] used to inject
     * callbacks at various stages of the kafka.javaapi.producer.AsyncProducer pipeline.
     * @param partitioner class that implements the kafka.javaapi.producer.Partitioner[K], used to supply a custom
     * partitioning strategy on the message key (of type K) that is specified through the ProducerData[K, T]
     * object in the  send API
     */
    public Producer(ProducerConfig config,
                    Encoder<V> encoder,
                    kafka.javaapi.producer.async.EventHandler<V> eventHandler,
                    kafka.javaapi.producer.async.CallbackHandler<V> cbkHandler,
                    Partitioner<K> partitioner)  {
        init(config, partitioner,
                new ProducerPool<V>(config, encoder,
                        new kafka.producer.async.EventHandler<V>(){
                            public void init(Properties props){
                                eventHandler.init(props);
                            }
                            public void handle(QueueItem<V>[] events, SyncProducer producer, Encoder<V> encoder)  throws Exception{
                                eventHandler.handle(Stream.of(events).collect(Collectors.toList()), new kafka.javaapi.producer.SyncProducer(producer), encoder);
                            }
                            public void close(){
                                eventHandler.close();
                            }
                        }
                        ,
                        new kafka.producer.async.CallbackHandler<V>(){
                            public void init(Properties props){
                                cbkHandler.init(props);
                            }
                            public QueueItem<V> beforeEnqueue(QueueItem<V> data){
                               return cbkHandler.beforeEnqueue(data);
                            }

                            public  void afterEnqueue(QueueItem<V> data, boolean added){
                                cbkHandler.afterEnqueue(data, added);
                            }
                            public List<QueueItem<V>> afterDequeuingExistingData(QueueItem<V> data){
                                return cbkHandler.afterDequeuingExistingData(data);
                            }
                            public  List<QueueItem<V>> beforeSendingData(QueueItem<V>[] data){
                                return cbkHandler.beforeSendingData(Stream.of(data).collect(Collectors.toList()));
                            }
                            public  List<QueueItem<V>> lastBatchBeforeClose(){
                                return  cbkHandler.lastBatchBeforeClose();
                            }
                            public void close(){
                                cbkHandler.close();
                            }
                        }


       ),true);
    }
    private kafka.producer.Producer<K,V> underlying ;

    private void init(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool, boolean populateProducerPool){
        this.config = config;
        this.partitioner = partitioner;
        this.producerPool = producerPool;
        this.populateProducerPool = populateProducerPool;
        underlying = new kafka.producer.Producer<K,V>(config, partitioner, producerPool, populateProducerPool, null);
    }

    /**
     * Sends the data to a single topic, partitioned by key, using either the
     * synchronous or the asynchronous producer
     * @param producerData the producer data object that encapsulates the topic, key and message data
     */
    public void send( kafka.javaapi.producer.ProducerData<K,V> producerData) throws Exception{
        underlying.send(new kafka.producer.ProducerData<K,V>(producerData.getTopic(), producerData.getKey(),
                producerData.getData()));
    }

    /**
     * Use this API to send data to multiple topics
     * @param producerDatas list of producer data objects that encapsulate the topic, key and message data
     */
    public void  send(List<kafka.javaapi.producer.ProducerData<K,V>> producerDatas) throws Exception{
        for(kafka.javaapi.producer.ProducerData<K,V> producerData:producerDatas){
            underlying.send(new kafka.producer.ProducerData<K,V>(producerData.getTopic(), producerData.getKey(),
                    producerData.getData()));
        }
    }

    /**
     * Close API to close the producer pool connections to all Kafka brokers. Also closes
     * the zookeeper client connection if one exists
     */
    public void close()throws InterruptedException{
        underlying.close();
    }
}

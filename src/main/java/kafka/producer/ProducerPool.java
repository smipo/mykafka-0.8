package kafka.producer;

import kafka.api.ProducerRequest;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.common.InvalidConfigException;
import kafka.common.UnavailableProducerException;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.async.*;
import kafka.serializer.Encoder;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ProducerPool<V> {

    private static Logger logger = Logger.getLogger(ProducerPool.class);

    private ProducerConfig config;
    private Encoder<V> serializer;
    private ConcurrentMap<Integer, SyncProducer> syncProducers;
    private ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;
    private EventHandler<V> inputEventHandler = null;
    private CallbackHandler<V> cbkHandler = null;

    public ProducerPool(ProducerConfig config, Encoder<V> serializer, ConcurrentMap<Integer, SyncProducer> syncProducers, ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers, EventHandler<V> inputEventHandler, CallbackHandler<V> cbkHandler) {
        this.config = config;
        this.serializer = serializer;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        this.inputEventHandler = inputEventHandler;
        this.cbkHandler = cbkHandler;

        eventHandler = inputEventHandler;
        if (eventHandler == null)
            eventHandler = new DefaultEventHandler(config, cbkHandler);
        if (serializer == null)
            throw new InvalidConfigException("serializer passed in is null!");

        if ("sync".equals(config.producerType)) {
            sync = true;
        } else if ("async".equals(config.producerType)) {
            sync = false;
        } else {
            throw new InvalidConfigException("Valid values for producer.type are sync/async");
        }
    }

    public ProducerPool(ProducerConfig config, Encoder<V> serializer,
                        EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler) {
        this(config, serializer,
                new ConcurrentHashMap<Integer, SyncProducer>(),
                new ConcurrentHashMap<Integer, AsyncProducer<V>>(),
                eventHandler, cbkHandler);
    }

    public ProducerPool(ProducerConfig config,Encoder<V> serializer) throws ClassNotFoundException,InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        init(config,serializer,
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                Utils.getObject(config.eventHandler) == null?null:(EventHandler<V>)Utils.getObject(config.eventHandler),
                Utils.getObject(config.cbkHandler) == null?null:(CallbackHandler<V>)Utils.getObject(config.cbkHandler));
    }

    private void init(ProducerConfig config, Encoder<V> serializer, ConcurrentMap<Integer, SyncProducer> syncProducers, ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers, EventHandler<V> inputEventHandler, CallbackHandler<V> cbkHandler){
        this.config = config;
        this.serializer = serializer;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        this.inputEventHandler = inputEventHandler;
        this.cbkHandler = cbkHandler;

        eventHandler = inputEventHandler;
        if (eventHandler == null)
            eventHandler = new DefaultEventHandler(config, cbkHandler);
        if (serializer == null)
            throw new InvalidConfigException("serializer passed in is null!");

        if ("sync".equals(config.producerType)) {
            sync = true;
        } else if ("async".equals(config.producerType)) {
            sync = false;
        } else {
            throw new InvalidConfigException("Valid values for producer.type are sync/async");
        }
    }
    private EventHandler<V>  eventHandler;

    private boolean sync;


    /**
     * add a new producer, either synchronous or asynchronous, connecting
     * to the specified broker
     * @param broker bid the id of the broker
     * @param broker host the hostname of the broker
     * @param broker port the port of the broker
     */
    public void addProducer(Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host());
        props.put("port", broker.port() + "");
        props.putAll(config.props);
        if(sync) {
            SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
            logger.info("Creating sync producer for broker id = " + broker.id() + " at " + broker.host() + ":" + broker.port());
            syncProducers.put(broker.id(), producer);
        } else {
            AsyncProducer producer = new AsyncProducer<V>(new AsyncProducerConfig(props),
                    new SyncProducer(new SyncProducerConfig(props)),
                    serializer,
                    eventHandler, config.eventHandlerProps,
                    cbkHandler, config.cbkHandlerProps);
            producer.start();
            logger.info("Creating async producer for broker id = " + broker.id() + " at " + broker.host() + ":" + broker.port());
            asyncProducers.put(broker.id(), producer);
        }
    }

    /**
     * selects either a synchronous or an asynchronous producer, for
     * the specified broker id and calls the send API on the selected
     * producer to publish the data to the specified broker partition
     * @param poolData the producer pool request object
     */
    public void send(ProducerPoolData<V>... poolData) throws Exception{
        Set<Integer> distinctBrokers = new HashSet<>();
        List<ProducerPoolData<V>> remainingRequests = new ArrayList<>();
        for (ProducerPoolData<V> pd : poolData) {
            distinctBrokers.add(pd.bidPid.brokerId());
            remainingRequests.add(pd);
        }
        Iterator<Integer> iterator = distinctBrokers.iterator();
        while (iterator.hasNext()) {
            Integer bid = iterator.next();
            List<ProducerPoolData<V>> requestsForThisBid = remainingRequests.stream().filter(t -> t.getBidPid().brokerId() == bid).collect(Collectors.toList());
            remainingRequests = remainingRequests.stream().filter(t -> t.getBidPid().brokerId() != bid).collect(Collectors.toList());
            if (sync) {
                List<ProducerRequest> producerRequests = new ArrayList<>();
                for (ProducerPoolData<V> req : requestsForThisBid) {
                    Message[] messages = new Message[req.getData().size()];
                    for (int i = 0; i < req.getData().size(); i++) {
                        messages[i] = serializer.toMessage(req.getData().get(i));
                    }
                    producerRequests.add(new ProducerRequest(req.getTopic(), req.getBidPid().partId(),
                            new ByteBufferMessageSet(config.compressionCodec,
                                    messages)));
                }
                logger.debug("Fetching sync producer for broker id: " + bid);
                SyncProducer producer = syncProducers.get(bid);
                if (producer != null) {
                    if (producerRequests.size() > 1) {
                        ProducerRequest[] producerRequests1 = new ProducerRequest[producerRequests.size()];
                        producer.multiSend(producerRequests.toArray(producerRequests1));
                    }
                    else
                        producer.send(producerRequests.get(0).topic(),
                                producerRequests.get(0).partition(),
                                producerRequests.get(0).messages());
                } else
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Sync Producer for broker " + bid + " does not exist in the pool");
            } else {
                logger.debug("Fetching async producer for broker id: " + bid);
                AsyncProducer<V> producer = asyncProducers.get(bid);
                if (producer != null) {
                    for(ProducerPoolData<V> req:requestsForThisBid){
                        for(V d:req.getData()){
                            producer.send(req.getTopic(), d, req.getBidPid().partId());
                        }
                    }
                } else
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Async Producer for broker " + bid + " does not exist in the pool");
            }
        }
    }
    /**
     * Closes all the producers in the pool
     */
    public void close() throws InterruptedException {
        if("sync".equals(config.producerType)){
            logger.info("Closing all sync producers");
            Iterator<SyncProducer> iter = syncProducers.values().iterator();
            while(iter.hasNext())
                iter.next().close();
        }else if("async".equals(config.producerType)){
            logger.info("Closing all async producers");
            Iterator<AsyncProducer<V>>  iter = asyncProducers.values().iterator();
            while(iter.hasNext())
                iter.next().close();
        }
    }

    /**
     * This constructs and returns the request object for the producer pool
     * @param topic the topic to which the data should be published
     * @param bidPid the broker id and partition id
     * @param data the data to be published
     */
    public <V> ProducerPoolData<V> getProducerPoolData(String topic ,Partition bidPid,List<V>  data) {
        return new ProducerPoolData<>(topic, bidPid, data);
    }

    public class ProducerPoolData<V>{
        String topic;
        Partition bidPid;
        List<V> data;

        public ProducerPoolData(String topic, Partition bidPid, List<V> data) {
            this.topic = topic;
            this.bidPid = bidPid;
            this.data = data;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Partition getBidPid() {
            return bidPid;
        }

        public void setBidPid(Partition bidPid) {
            this.bidPid = bidPid;
        }

        public List<V> getData() {
            return data;
        }

        public void setData(List<V> data) {
            this.data = data;
        }
    }
}

package kafka.controller;

import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.server.KafkaConfig;
import kafka.utils.Pair;
import kafka.utils.ShutdownableThread;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class ControllerChannelManager {

    private static Logger logger = Logger.getLogger(ControllerChannelManager.class);

    ControllerContext controllerContext;
    KafkaConfig config;

    public ControllerChannelManager(ControllerContext controllerContext, KafkaConfig config) {
        this.controllerContext = controllerContext;
        this.config = config;

        controllerContext.liveBrokers.foreach(addNewBroker(_));
    }

    private Map<Integer,ControllerBrokerStateInfo> brokerStateInfo = new HashMap<>();
    private Object brokerLock = new Object();


    public void startup()  {
         synchronized(brokerLock) {
            brokerStateInfo.forEach((k,v) -> startRequestSendThread(k));
        }
    }

    public void shutdown()  {
         synchronized(brokerLock) {
            brokerStateInfo.forEach((k,v) -> removeExistingBroker(k));
        }
    }

    public void sendRequest(int brokerId , RequestOrResponse request , Callback callback) throws InterruptedException {
         synchronized(brokerLock) {
             ControllerBrokerStateInfo stateInfoOpt = brokerStateInfo.get(brokerId);
             if(stateInfoOpt != null){
                 stateInfoOpt.messageQueue.put(new Pair<>(request, callback));
             }else{
                 logger.warn("Not sending request %s to broker %d, since it is offline.".format(request + "", brokerId));
             }
        }
    }

    public void addBroker(Broker broker) throws IOException {
        // be careful here. Maybe the startup() API has already started the request send thread
        synchronized(brokerLock) {
            if(!brokerStateInfo.containsKey(broker.id())) {
                addNewBroker(broker);
                startRequestSendThread(broker.id());
            }
        }
    }

    public void removeBroker(int brokerId) {
        synchronized(brokerLock) {
            removeExistingBroker(brokerId);
        }
    }

    private void addNewBroker(Broker broker) throws IOException {
        BlockingQueue messageQueue = new LinkedBlockingQueue<Pair<RequestOrResponse,Callback>>(config.controllerMessageQueueSize);
        logger.debug("Controller %d trying to connect to broker %d".format(config.brokerId + "",broker.id()));
        BlockingChannel channel = new BlockingChannel(broker.host(), broker.port(),
                BlockingChannel.UseDefaultBufferSize,
                BlockingChannel.UseDefaultBufferSize,
                config.controllerSocketTimeoutMs);
        channel.connect();
        RequestSendThread requestThread = new RequestSendThread(config.brokerId, controllerContext, broker.id(), messageQueue, channel);
        requestThread.setDaemon(false);
        brokerStateInfo.put(broker.id(), new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread));
    }

    private void removeExistingBroker(int brokerId) {
        try {
            brokerStateInfo.get(brokerId).channel.disconnect();
            brokerStateInfo.get(brokerId).requestSendThread.shutdown();
            brokerStateInfo.remove(brokerId);
        }catch (Throwable e){
          logger.error("Error while removing broker by the controller", e);
        }
    }

    private void startRequestSendThread(int brokerId) {
        RequestSendThread requestThread = brokerStateInfo.get(brokerId).requestSendThread;
        if(requestThread.getState() == Thread.State.NEW)
            requestThread.start();
    }

    public  class RequestSendThread  extends ShutdownableThread {
        public int controllerId;
        public ControllerContext controllerContext;
        public int toBrokerId;
        public BlockingQueue<Pair<RequestOrResponse, Callback<RequestOrResponse>>> queue;
        public BlockingChannel channel;

        public RequestSendThread(int controllerId, ControllerContext controllerContext, int toBrokerId, BlockingQueue<Pair<RequestOrResponse, Callback<RequestOrResponse>>> queue, BlockingChannel channel) {
            super("Controller-%d-to-broker-%d-send-thread".format(controllerId + "", toBrokerId),true);
            this.controllerId = controllerId;
            this.controllerContext = controllerContext;
            this.toBrokerId = toBrokerId;
            this.queue = queue;
            this.channel = channel;
        }
        private Object lock = new Object();

        @Override
        public void doWork() throws IOException, InterruptedException {
            Pair<RequestOrResponse, Callback<RequestOrResponse>> queueItem = queue.take();
            RequestOrResponse request = queueItem.getKey();
            Callback<RequestOrResponse> callback = queueItem.getValue();

            Receive receive  = null;

            try{
                synchronized(lock) {
                    channel.connect(); // establish a socket connection if needed
                    channel.send(request);
                    receive = channel.receive();
                    RequestOrResponse response = null;
                    Short requestId = request.requestId;
                    if(requestId == RequestKeys.LeaderAndIsrKey){
                        response = LeaderAndIsrResponse.readFrom(receive.buffer());
                    }else if(requestId == RequestKeys.StopReplicaKey){
                        response = StopReplicaResponse.readFrom(receive.buffer());
                    }else if(requestId == RequestKeys.UpdateMetadataKey){
                        response = UpdateMetadataResponse.readFrom(receive.buffer());
                    }
                    if(callback != null){
                        callback.onCallback(response);
                    }
                }
            } catch (Throwable e){
                logger.warn("Controller %d fails to send a request to broker %d".format(controllerId + "", toBrokerId), e);
                // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
                channel.disconnect();
            }
        }
    }
    public static class ControllerBrokerRequestBatch{
        public ControllerContext controllerContext;
        public Pair<RequestOrResponse, Callback<RequestOrResponse>> sendRequest;
        public int controllerId;
        public String clientId;

        public ControllerBrokerRequestBatch(ControllerContext controllerContext, Pair<RequestOrResponse, Callback<RequestOrResponse>> sendRequest, int controllerId, String clientId) {
            this.controllerContext = controllerContext;
            this.sendRequest = sendRequest;
            this.controllerId = controllerId;
            this.clientId = clientId;
        }

        public Map<Integer,Map<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo>> leaderAndIsrRequestMap = new HashMap<>();
        public Map<Integer, List<Pair<String,Integer>>> stopReplicaRequestMap = new HashMap<>();
        public Map<Integer, List<Pair<String,Integer>>> stopAndDeleteReplicaRequestMap = new HashMap<>();
        public Map<Integer,Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo>> updateMetadataRequestMap = new HashMap<>();

        public  void newBatch() {
            // raise error if the previous batch is not empty
            if(leaderAndIsrRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
                        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()));
            if(stopReplicaRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()));
            if(updateMetadataRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()));
            if(stopAndDeleteReplicaRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        "new one. Some StopReplica with delete state changes %s might be lost ".format(stopAndDeleteReplicaRequestMap.toString()));
            leaderAndIsrRequestMap.clear();
            stopReplicaRequestMap.clear();
            updateMetadataRequestMap.clear();
            stopAndDeleteReplicaRequestMap.clear();
        }

        public void addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, String topic ,int partition,
                                                     LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch ,
                                                     List<Integer> replicas) {
            for(Integer brokerId:brokerIds){
                leaderAndIsrRequestMap.putIfAbsent(brokerId, new HashMap<Pair<String, Integer>, LeaderAndIsrRequest.PartitionStateInfo>());
                leaderAndIsrRequestMap.get(brokerId).put((topic, partition),
                       new LeaderAndIsrRequest.PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.stream().collect(Collectors.toSet()));
            }
            addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(TopicAndPartition(topic, partition)));
        }

        public void addStopReplicaRequestForBrokers(List<Integer> brokerIds, String topic,int partition,boolean deletePartition) {
            for(Integer brokerId:brokerIds){
                stopReplicaRequestMap.putIfAbsent(brokerId, new ArrayList<>());
                stopAndDeleteReplicaRequestMap.putIfAbsent(brokerId, new ArrayList<>());
                if (deletePartition) {
                    List<Pair<String,Integer>> v = stopAndDeleteReplicaRequestMap.get(brokerId);
                    List<Pair<String,Integer>> v2 = stopAndDeleteReplicaRequestMap.get(brokerId);
                    v.addAll(v2);
                }
                else {
                    List<Pair<String,Integer>> v = stopReplicaRequestMap.get(brokerId);
                    List<Pair<String,Integer>> v2 = stopReplicaRequestMap.get(brokerId) ;
                    v.addAll(v2);
                }
            }
        }

        public void addUpdateMetadataRequestForBrokers(List<Integer> brokerIds,
                                               Set<TopicAndPartition> partitions) {
            Set<TopicAndPartition> partitionList = new HashSet<>();
            if(partitions == null || partitions.isEmpty()) {
                partitionList = controllerContext.partitionLeadershipInfo.keySet;
            } else {
                partitionList = partitions;
            }
            for(TopicAndPartition partition:partitionList){
                val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo().get(partition);
                leaderIsrAndControllerEpochOpt match {
                    case Some(leaderIsrAndControllerEpoch) =>
                        val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
                        val partitionStateInfo = PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
                        brokerIds.foreach { brokerId =>
                        updateMetadataRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[TopicAndPartition, PartitionStateInfo])
                        updateMetadataRequestMap(brokerId).put(partition, partitionStateInfo)
                    }
                    case None =>
                        info("Leader not assigned yet for partition %s. Skip sending udpate metadata request".format(partition))
                }
            }
        }

        public void sendRequestsToBrokers(int controllerEpoch, int correlationId) {
            for(Map.Entry<Integer,Map<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo>> entry : leaderAndIsrRequestMap.entrySet()){
                int broker = entry.getKey();
                Map<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos = entry.getValue();
                List<Integer> leaderIds = new ArrayList<>();
                for(Map.Entry<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> entryPartition : partitionStateInfos.entrySet()){
                    int leaderId = entryPartition.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader;
                    leaderIds.add(leaderId);
                }
                Set<Broker> leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id));
                LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId);
                for(Map.Entry<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> entryPartition : partitionStateInfos.entrySet()){
                    String typeOfRequest ;
                    if (broker == entryPartition.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader) typeOfRequest = "become-leader" ;
                    else typeOfRequest = "become-follower";
                    logger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request with correlationId %d to broker %d " +
                            "for partition [%s,%d]").format(controllerId + "", controllerEpoch, typeOfRequest, correlationId, broker,
                            entryPartition.getKey().getKey(), entryPartition.getKey().getValue()));
                }
                sendRequest(broker, leaderAndIsrRequest, null);
            }
            leaderAndIsrRequestMap.clear();
            updateMetadataRequestMap.foreach { m =>
                val broker = m._1
                val partitionStateInfos = m._2.toMap
                val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
                        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers)
                partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request with " +
                        "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, correlationId, broker, p._1)))
                sendRequest(broker, updateMetadataRequest, null)
            }
            updateMetadataRequestMap.clear()
            Seq((stopReplicaRequestMap, false), (stopAndDeleteReplicaRequestMap, true)) foreach {
                case(m, deletePartitions) => {
                    m foreach {
                        case(broker, replicas) =>
                            if (replicas.size > 0) {
                                debug("The stop replica request (delete = %s) sent to broker %d is %s"
                                        .format(deletePartitions, broker, replicas.mkString(",")))
                                val stopReplicaRequest = new StopReplicaRequest(deletePartitions, Set.empty[(String, Int)] ++ replicas, controllerId,
                                        controllerEpoch, correlationId)
                                sendRequest(broker, stopReplicaRequest, null)
                            }
                    }
                    m.clear()
                }
            }
        }
    }
    public static class ControllerBrokerStateInfo{
        public  BlockingChannel channel;
        public Broker broker;
        public BlockingQueue<Pair<RequestOrResponse, Callback<RequestOrResponse>>>  messageQueue;
        public RequestSendThread requestSendThread;

        public ControllerBrokerStateInfo(BlockingChannel channel, Broker broker, BlockingQueue<Pair<RequestOrResponse, Callback<RequestOrResponse>>> messageQueue, RequestSendThread requestSendThread) {
            this.channel = channel;
            this.broker = broker;
            this.messageQueue = messageQueue;
            this.requestSendThread = requestSendThread;
        }
    }
}

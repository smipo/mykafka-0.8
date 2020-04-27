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

    public ControllerChannelManager(ControllerContext controllerContext, KafkaConfig config)  {
        this.controllerContext = controllerContext;
        this.config = config;
    }

    private Map<Integer,ControllerBrokerStateInfo> brokerStateInfo = new HashMap<>();
    private Object brokerLock = new Object();


    public void startup()  throws IOException{
         synchronized(brokerLock) {
             for(Broker b:controllerContext.liveBrokers()){
                 addNewBroker(b);
             }
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
                 logger.warn(String.format("Not sending request %s to broker %d, since it is offline.",request , brokerId));
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
        logger.debug(String.format("Controller %d trying to connect to broker %d",config.brokerId,broker.id()));
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
            super(String.format("Controller-%d-to-broker-%d-send-thread",controllerId, toBrokerId),true);
            this.controllerId = controllerId;
            this.controllerContext = controllerContext;
            this.toBrokerId = toBrokerId;
            this.queue = queue;
            this.channel = channel;
        }
        private Object lock = new Object();

        @Override
        public void doWork() throws Throwable {
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
                logger.warn(String.format("Controller %d fails to send a request to broker %d",controllerId, toBrokerId), e);
                // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
                channel.disconnect();
            }
        }
    }
    public  class ControllerBrokerRequestBatch{
        public ControllerContext controllerContext;
        public int controllerId;
        public String clientId;

        public ControllerBrokerRequestBatch(ControllerContext controllerContext,  int controllerId, String clientId) {
            this.controllerContext = controllerContext;
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
                        String.format("a new one. Some LeaderAndIsr state changes %s might be lost ",leaderAndIsrRequestMap.toString()));
            if(stopReplicaRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        String.format("new one. Some StopReplica state changes %s might be lost ",stopReplicaRequestMap.toString()));
            if(updateMetadataRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        String.format("new one. Some UpdateMetadata state changes %s might be lost ",updateMetadataRequestMap.toString()));
            if(stopAndDeleteReplicaRequestMap.size() > 0)
                throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                        String.format("new one. Some StopReplica with delete state changes %s might be lost ",stopAndDeleteReplicaRequestMap.toString()));
            leaderAndIsrRequestMap.clear();
            stopReplicaRequestMap.clear();
            updateMetadataRequestMap.clear();
            stopAndDeleteReplicaRequestMap.clear();
        }

        public void addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, String topic ,int partition,
                                                     LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch ,
                                                     List<Integer> replicas) {
            for(Integer brokerId:brokerIds){
                leaderAndIsrRequestMap.putIfAbsent(brokerId, new HashMap<>());
                leaderAndIsrRequestMap.get(brokerId).put(new Pair<>(topic, partition),
                       new LeaderAndIsrRequest.PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.stream().collect(Collectors.toSet())));
            }
            Set<TopicAndPartition> set = new HashSet<>();
            set.add(new TopicAndPartition(topic, partition));
            addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds().stream().collect(Collectors.toList()), set);
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
                partitionList = controllerContext.partitionLeadershipInfo.keySet();
            } else {
                partitionList = partitions;
            }
            for(TopicAndPartition partition:partitionList){
                LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(partition);
                if(leaderIsrAndControllerEpoch != null){
                    List<Integer> replicas = controllerContext.partitionReplicaAssignment.get(partition);
                    LeaderAndIsrRequest.PartitionStateInfo partitionStateInfo = new LeaderAndIsrRequest.PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.stream().collect(Collectors.toSet()));
                    for(Integer brokerId:brokerIds){
                        updateMetadataRequestMap.putIfAbsent(brokerId, new HashMap<>());
                        updateMetadataRequestMap.get(brokerId).put(partition, partitionStateInfo);
                    }

                }else{
                    logger.info(String.format("Leader not assigned yet for partition %s. Skip sending udpate metadata request",partition.toString()));
                }
            }
        }

        public void sendRequestsToBrokers(int controllerEpoch, int correlationId) throws InterruptedException {
            for(Map.Entry<Integer,Map<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo>> entry : leaderAndIsrRequestMap.entrySet()){
                int broker = entry.getKey();
                Map<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos = entry.getValue();
                List<Integer> leaderIds = new ArrayList<>();
                for(Map.Entry<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> entryPartition : partitionStateInfos.entrySet()){
                    int leaderId = entryPartition.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader;
                    leaderIds.add(leaderId);
                }
                Set<Broker> leaders = controllerContext.liveOrShuttingDownBrokers().stream().filter(b -> leaderIds.contains(b.id())).collect(Collectors.toSet());
                LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId);
                for(Map.Entry<Pair<String,Integer>, LeaderAndIsrRequest.PartitionStateInfo> entryPartition : partitionStateInfos.entrySet()){
                    String typeOfRequest ;
                    if (broker == entryPartition.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader) typeOfRequest = "become-leader" ;
                    else typeOfRequest = "become-follower";
                    logger.trace(String.format("Controller %d epoch %d sending %s LeaderAndIsr request with correlationId %d to broker %d " +
                                    "for partition [%s,%d]",controllerId , controllerEpoch, typeOfRequest, correlationId, broker,
                            entryPartition.getKey().getKey(), entryPartition.getKey().getValue()));
                }
                sendRequest(broker, leaderAndIsrRequest, null);
            }
            leaderAndIsrRequestMap.clear();
            for(Map.Entry<Integer,Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo>> entry : updateMetadataRequestMap.entrySet()){
                int broker = entry.getKey();
                Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfos = entry.getValue();
                UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
                        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers());
                partitionStateInfos.forEach((k,v) -> logger.trace(String.format("Controller %d epoch %d sending UpdateMetadata request with " +
                        "correlationId %d to broker %d for partition %s",controllerId, controllerEpoch, correlationId, broker, k)));
                sendRequest(broker, updateMetadataRequest, null);
            }
            updateMetadataRequestMap.clear();

            for(Map.Entry<Integer, List<Pair<String,Integer>>> entry : stopReplicaRequestMap.entrySet()){
                if (entry.getValue().size() > 0) {
                    logger.debug(String
                            .format("The stop replica request (delete = %s) sent to broker %d is %s",false, entry.getKey(), entry.getValue().toString()));
                    StopReplicaRequest stopReplicaRequest = new StopReplicaRequest(false, entry.getValue().stream().collect(Collectors.toSet()), controllerId,
                            controllerEpoch, correlationId);
                    sendRequest(entry.getKey(), stopReplicaRequest, null);
                }
            }
            for(Map.Entry<Integer, List<Pair<String,Integer>>> entry : stopAndDeleteReplicaRequestMap.entrySet()){
                if (entry.getValue().size() > 0) {
                    logger.debug(String
                            .format("The stop replica request (delete = %s) sent to broker %d is %s",true, entry.getKey(), entry.getValue().toString()));
                    StopReplicaRequest stopReplicaRequest = new StopReplicaRequest(true, entry.getValue().stream().collect(Collectors.toSet()), controllerId,
                            controllerEpoch, correlationId);
                    sendRequest(entry.getKey(), stopReplicaRequest, null);
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

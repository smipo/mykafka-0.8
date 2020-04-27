package kafka.server;

import kafka.admin.CreateTopicCommand;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static kafka.api.RequestKeys.*;


public class KafkaApis {

    private static Logger logger = Logger.getLogger(KafkaApis.class);

    RequestChannel requestChannel;
    ReplicaManager replicaManager;
    ZkClient zkClient;
    int brokerId;
    KafkaController controller;

    public KafkaApis(RequestChannel requestChannel, ReplicaManager replicaManager, ZkClient zkClient, int brokerId, KafkaController controller) {
        this.requestChannel = requestChannel;
        this.replicaManager = replicaManager;
        this.zkClient = zkClient;
        this.brokerId = brokerId;
        this.controller = controller;

        producerRequestPurgatory =
                new ProducerRequestPurgatory(replicaManager.config.producerPurgatoryPurgeIntervalRequests);

        fetchRequestPurgatory =
                new FetchRequestPurgatory(requestChannel, replicaManager.config.fetchPurgatoryPurgeIntervalRequests);
    }

    private ProducerRequestPurgatory producerRequestPurgatory ;

    private FetchRequestPurgatory fetchRequestPurgatory;
    /* following 3 data structures are updated by the update metadata request
     * and is queried by the topic metadata request. */
    Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> leaderCache = new HashMap<>();
    private Map<Integer, Broker> aliveBrokers = new HashMap<>();
    private Object partitionMetadataLock = new Object();


    /**
     * Top-level method that handles all requests and multiplexes to the right api
     */
    public void handle(RequestChannel.Request request) throws IOException, InterruptedException {
        try{
            logger.trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress);
            if(request.requestId == ProduceKey) handleProducerRequest(request);
            else if(request.requestId == FetchKey) handleFetchRequest(request);
            else if(request.requestId == OffsetsKey) handleOffsetRequest(request);
            else if(request.requestId == MetadataKey) handleTopicMetadataRequest(request);
            else if(request.requestId == LeaderAndIsrKey) handleLeaderAndIsrRequest(request);
            else if(request.requestId == StopReplicaKey)  handleStopReplicaRequest(request);
            else if(request.requestId == UpdateMetadataKey)handleUpdateMetadataRequest(request);
            else if(request.requestId == ControlledShutdownKey)handleControlledShutdownRequest(request);
            else throw new KafkaException("No mapping found for handler id " + request.requestId);
        } catch (Throwable e){
            request.requestObj.handleError(e, requestChannel, request);
            logger.error(String.format("error when handling request %s",request.requestObj), e);
        }
    }

    public void handleLeaderAndIsrRequest(RequestChannel.Request request) throws InterruptedException, IOException{
        LeaderAndIsrRequest leaderAndIsrRequest = (LeaderAndIsrRequest)request.requestObj;
        try {
            Pair<Map<Pair<String, Integer>,Short>, Short> pair = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest);
            LeaderAndIsrResponse leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, pair.getKey(), pair.getValue());
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)));
        } catch (KafkaStorageException e){
            logger.fatal("Disk error during leadership change.", e);
            Runtime.getRuntime().halt(1);
        }
    }

    public void handleStopReplicaRequest(RequestChannel.Request request) throws InterruptedException, IOException {
        StopReplicaRequest stopReplicaRequest = (StopReplicaRequest)request.requestObj;
        Pair<Map<Pair<String, Integer>,Short>, Short> pair = replicaManager.stopReplicas(stopReplicaRequest);
        StopReplicaResponse stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, pair.getKey(), pair.getValue());
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(stopReplicaResponse)));
        replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads();
    }

    public void handleUpdateMetadataRequest(RequestChannel.Request request) throws IOException, InterruptedException {
        UpdateMetadataRequest updateMetadataRequest = (UpdateMetadataRequest)request.requestObj;
        if(updateMetadataRequest.controllerEpoch < replicaManager.controllerEpoch) {
            String stateControllerEpochErrorMessage = String.format("Broker %d received update metadata request with correlation id %d from an " +
                            "old controller %d with epoch %d. Latest known controller epoch is %d",brokerId,
                    updateMetadataRequest.correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
                    replicaManager.controllerEpoch);
            logger.warn(stateControllerEpochErrorMessage);
            throw new ControllerMovedException(stateControllerEpochErrorMessage);
        }
        synchronized(partitionMetadataLock) {
            replicaManager.controllerEpoch = updateMetadataRequest.controllerEpoch;
            // cache the list of alive brokers in the cluster
            updateMetadataRequest.aliveBrokers.forEach(b -> aliveBrokers.put(b.id(), b));
            for(Map.Entry<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> entry : updateMetadataRequest.partitionStateInfos.entrySet()){
                leaderCache.put(entry.getKey(), entry.getValue());
                if(logger.isTraceEnabled())
                    logger.trace(String.format("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
                                    "sent by controller %d epoch %d with correlation id %d",brokerId, entry.getValue().toString(), entry.getKey().toString(),
                            updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId));
            }
        }
        UpdateMetadataResponse updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId,ErrorMapping.NoError);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(updateMetadataResponse)));
    }

    public void handleControlledShutdownRequest(RequestChannel.Request request) throws IOException, InterruptedException {
        ControlledShutdownRequest controlledShutdownRequest = (ControlledShutdownRequest)request.requestObj;
        Set<TopicAndPartition> partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId);
        ControlledShutdownResponse controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
                ErrorMapping.NoError, partitionsRemaining);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(controlledShutdownResponse)));
    }

    /**
     * Check if a partitionData from a produce request can unblock any
     * DelayedFetch requests.
     */
    public void maybeUnblockDelayedFetchRequests(String topic ,int partition,int messageSizeInBytes) throws Exception {
        List<DelayedFetch> satisfied =  fetchRequestPurgatory.update(new RequestKey(topic, partition), messageSizeInBytes);
        logger.trace(String.format("Producer request to (%s-%d) unblocked %d fetch requests.",topic, partition, satisfied.size()));

        // send any newly unblocked responses
        for(DelayedFetch fetchReq : satisfied) {
            Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> topicData = readMessageSets(fetchReq.fetch);
            FetchResponse response = new FetchResponse(fetchReq.fetch.correlationId, topicData);
            requestChannel.sendResponse(new RequestChannel.Response(fetchReq.request, new FetchResponse.FetchResponseSend(response)));
        }
    }

    /**
     * Handle a produce request
     */
    public void handleProducerRequest(RequestChannel.Request request) throws Exception {
        ProducerRequest produceRequest = (ProducerRequest)request.requestObj;
        long sTime = System.currentTimeMillis();
        List<ProduceResult> localProduceResults = appendToLocalLog(produceRequest);
        logger.debug(String.format("Produce to local log in %d ms",System.currentTimeMillis() - sTime));
        int numPartitionsInError= 0;
        for(ProduceResult p :localProduceResults){
            if(p.error != null) numPartitionsInError++;
        }
        for(Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : produceRequest.data.entrySet()){
            maybeUnblockDelayedFetchRequests(entry.getKey().topic(), entry.getKey().partition(),entry.getValue().sizeInBytes());
        }
        boolean allPartitionHaveReplicationFactorOne = false;
        for(TopicAndPartition t:produceRequest.data.keySet()){
            if(replicaManager.getReplicationFactorForPartition(t.topic(), t.partition()) != 1) allPartitionHaveReplicationFactorOne = true;
        }
        if(produceRequest.requiredAcks == 0) {
            // no operation needed if producer request.required.acks = 0; however, if there is any exception in handling the request, since
            // no response is expected by the producer the handler will send a close connection response to the socket server
            // to close the socket so that the producer client will know that some exception has happened and will refresh its metadata
            if (numPartitionsInError != 0) {
                logger.info(String
                        .format("Send the close connection response due to error handling produce request " +
                                "[clientId = %s, correlationId = %s, topicAndPartition = %s] with Ack=0",produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet().toString()));
                requestChannel.closeConnection(request.processor, request);
            } else {
                requestChannel.noOperation(request.processor, request);
            }
        } else if (produceRequest.requiredAcks == 1 ||
                produceRequest.numPartitions() <= 0 ||
                allPartitionHaveReplicationFactorOne ||
                numPartitionsInError == produceRequest.numPartitions()) {
            Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus> status = new HashMap<>();
            for(ProduceResult p :localProduceResults){
                status.put(p.key,new ProducerResponse.ProducerResponseStatus(p.errorCode(), p.start));
            }
            ProducerResponse response = new ProducerResponse(produceRequest.correlationId, status);
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        } else {
            // create a list of (topic, partition) pairs to use as keys for this delayed request
            List<RequestKey> producerRequestKeys = produceRequest.data.keySet().stream().map(
                    topicAndPartition -> new RequestKey(topicAndPartition)).collect(Collectors.toList());
            Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus> status = new HashMap<>();
            for(ProduceResult p :localProduceResults){
                status.put(p.key,new ProducerResponse.ProducerResponseStatus(p.errorCode(), p.start));
            }
            DelayedProduce delayedProduce = new DelayedProduce(producerRequestKeys,
                    request,
                    status,
                    produceRequest,
                    produceRequest.ackTimeoutMs);
            producerRequestPurgatory.watch(delayedProduce);

            /*
             * Replica fetch requests may have arrived (and potentially satisfied)
             * delayedProduce requests while they were being added to the purgatory.
             * Here, we explicitly check if any of them can be satisfied.
             */
            List<DelayedProduce> satisfiedProduceRequests = new ArrayList<>();
            for(RequestKey r:producerRequestKeys){
                satisfiedProduceRequests.addAll(producerRequestPurgatory.update(r, r));
            }
            logger.debug(satisfiedProduceRequests.size() +
                    " producer requests unblocked during produce to local log.");
            for(DelayedProduce d:satisfiedProduceRequests){
                d.respond();
            }
            // we do not need the data anymore
            produceRequest.emptyData();
        }
    }

    /**
     * Helper method for handling a parsed producer request
     */
    private List<ProduceResult> appendToLocalLog(ProducerRequest producerRequest) {
        Map<TopicAndPartition, ByteBufferMessageSet>  partitionAndData  = producerRequest.data;
        logger.trace(String.format("Append [%s] to local log ",partitionAndData.toString()));
        List<ProduceResult> res = new ArrayList<>();
        for(Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : partitionAndData.entrySet()){
            TopicAndPartition topicAndPartition = entry.getKey();
            ByteBufferMessageSet messages = entry.getValue();
            try {
                Partition partition = replicaManager.getPartition(topicAndPartition.topic(), topicAndPartition.partition());
                if(partition == null){
                    throw new UnknownTopicOrPartitionException(String
                            .format("Partition %s doesn't exist on %d",topicAndPartition.toString(), brokerId));
                }
                Pair<Long,Long> pair = partition.appendMessagesToLeader(messages);
                logger.trace(String
                        .format("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d",messages.toString(), topicAndPartition.topic(), topicAndPartition.partition(), pair.getKey(), pair.getValue()));
                res.add(new ProduceResult(topicAndPartition, pair.getKey(), pair.getValue(),null));
            } catch (KafkaStorageException e){
                // NOTE: Failed produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
                // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request
                // for a partition it is the leader for
                logger.fatal("Halting due to unrecoverable I/O error while handling produce request: ", e);
                Runtime.getRuntime().halt(1);
            }catch (UnknownTopicOrPartitionException utpe){
                logger.warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, utpe.getMessage()));
                res.add(new ProduceResult(topicAndPartition, utpe));
            }catch (NotLeaderForPartitionException nle){
                logger.warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nle.getMessage()));
                res.add(new ProduceResult(topicAndPartition, nle));
            }catch (Throwable e){
                logger.error(String
                        .format("Error processing ProducerRequest with correlation id %d from client %s on partition %s",producerRequest.correlationId, producerRequest.clientId, topicAndPartition), e);
                res.add(new ProduceResult(topicAndPartition, e));
            }
        }
        return res;
    }

    /**
     * Handle a fetch request
     */
    public void handleFetchRequest(RequestChannel.Request request) throws Exception {
        FetchRequest fetchRequest = (FetchRequest)request.requestObj;
        if(fetchRequest.isFromFollower()) {
            maybeUpdatePartitionHw(fetchRequest);
            // after updating HW, some delayed produce requests may be unblocked
            List<DelayedProduce> satisfiedProduceRequests = new ArrayList<>();
            for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
                RequestKey key = new RequestKey(entry.getKey());
                satisfiedProduceRequests.addAll(producerRequestPurgatory.update(key, key));
            }
            logger.debug(String
                    .format("Replica %d fetch unblocked %d producer requests.",fetchRequest.replicaId, satisfiedProduceRequests.size()));
            for(DelayedProduce d:satisfiedProduceRequests){
                d.respond();
            }
        }
        int bytesReadable = 0;
        Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> dataRead = readMessageSets(fetchRequest);
        for(Map.Entry<TopicAndPartition, FetchResponse.FetchResponsePartitionData> entry : dataRead.entrySet()){
            bytesReadable += entry.getValue().messages.sizeInBytes();
        }
        if(fetchRequest.maxWait <= 0 ||
                bytesReadable >= fetchRequest.minBytes ||
                fetchRequest.numPartitions() <= 0) {
            logger.debug(String
                    .format("Returning fetch response %s for fetch request with correlation id %d to client %s",dataRead.values().toString(), fetchRequest.correlationId, fetchRequest.clientId));
            FetchResponse response = new FetchResponse(fetchRequest.correlationId, dataRead);
            requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponse.FetchResponseSend(response)));
        } else {
            logger.debug(String.format("Putting fetch request with correlation id %d from client %s into purgatory",fetchRequest.correlationId,
                    fetchRequest.clientId));
            // create a list of (topic, partition) pairs to use as keys for this delayed request
            List<RequestKey> delayedFetchKeys = fetchRequest.requestInfo.keySet().stream().map(x->new RequestKey(x)).collect(Collectors.toList());
            DelayedFetch delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest, fetchRequest.maxWait, bytesReadable);
            fetchRequestPurgatory.watch(delayedFetch);
        }
    }

    private void maybeUpdatePartitionHw(FetchRequest fetchRequest) throws IOException {
        logger.debug(String.format("Maybe update partition HW due to fetch request: %s ",fetchRequest.toString()));
        for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
            replicaManager.recordFollowerPosition(entry.getKey().topic(), entry.getKey().partition(), fetchRequest.replicaId, entry.getValue().offset);
        }
    }

    /**
     * Read from all the offset details given and return a map of
     * (topic, partition) -> PartitionData
     */
    private  Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> readMessageSets(FetchRequest fetchRequest)  {
        Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> resMap = new HashMap<>();
        boolean isFetchFromFollower = fetchRequest.isFromFollower();
        for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            long offset = entry.getValue().offset;
            long fetchSize = entry.getValue().fetchSize;
            FetchResponse.FetchResponsePartitionData partitionData ;
            try {
                Pair<MessageSet, Long> pair = readMessageSet(topic, partition, offset, (int)fetchSize, fetchRequest.replicaId);
                MessageSet messages = pair.getKey();
                long highWatermark = pair.getValue();
                if (!isFetchFromFollower) {
                    partitionData = new FetchResponse.FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages);
                } else {
                    logger.debug(String
                            .format("Leader %d for partition [%s,%d] received fetch request from follower %d",brokerId, topic, partition, fetchRequest.replicaId));
                    partitionData = new FetchResponse.FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages);
                }
            } catch (UnknownTopicOrPartitionException utpe){
                // NOTE: Failed fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
                // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request
                // for a partition it is the leader for
                logger.warn(String.format("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s",
                        fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage()));
                partitionData = new FetchResponse.FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass().getName()), -1L, MessageSet.Empty);
            }catch (NotLeaderForPartitionException nle){
                logger.warn(String.format("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s",
                        fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage()));
                partitionData =  new FetchResponse.FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass().getName()), -1L, MessageSet.Empty);
            }catch (Throwable t){
                logger.error(String
                        .format("Error when processing fetch request for partition [%s,%d] offset %d from %s with correlation id %d",topic, partition, offset, isFetchFromFollower == true? "follower" : "consumer", fetchRequest.correlationId), t);
                partitionData = new FetchResponse.FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass().getName()), -1L, MessageSet.Empty);
            }
            resMap.put(new TopicAndPartition(topic, partition), partitionData);
        }
        return resMap;
    }

    /**
     * Read from a single topic/partition at the given offset upto maxSize bytes
     */
    private Pair<MessageSet, Long> readMessageSet(String topic,
                                                  int partition,
                                                  long offset,
                                                  int maxSize,
                                                  int fromReplicaId) throws IOException {
        // check if the current broker is the leader for the partitions
        Replica localReplica ;
        if(fromReplicaId == RequestOrResponse.DebuggingConsumerId)
            localReplica = replicaManager.getReplicaOrException(topic, partition);
        else
            localReplica = replicaManager.getLeaderReplicaIfLocal(topic, partition);

        logger.trace("Fetching log segment for topic, partition, offset, size = " + topic + partition + offset + maxSize);
        Long maxOffsetOpt = null;
        if (fromReplicaId == RequestOrResponse.OrdinaryConsumerId) {
            maxOffsetOpt = localReplica.highWatermark();
        }
        MessageSet messages ;
        if(localReplica.log != null){
            messages = localReplica.log.read(offset, maxSize, maxOffsetOpt);
        }else{
            logger.error(String.format("Leader for partition [%s,%d] on broker %d does not have a local log",topic, partition, brokerId));
            messages = MessageSet.Empty;
        }
        return new Pair<>(messages, localReplica.highWatermark());
    }

    /**
     * Service the offset request API
     */
    public void handleOffsetRequest(RequestChannel.Request request) throws IOException, InterruptedException {
        OffsetRequest offsetRequest = (OffsetRequest)request.requestObj;
        Map<TopicAndPartition, OffsetResponse.PartitionOffsetsResponse> responseMap = new HashMap<>();
        for(Map.Entry<TopicAndPartition, OffsetRequest.PartitionOffsetRequestInfo> entry : offsetRequest.requestInfo.entrySet()){
            TopicAndPartition topicAndPartition = entry.getKey();
            OffsetRequest.PartitionOffsetRequestInfo partitionOffsetRequestInfo = entry.getValue();
            try {
                // ensure leader exists
                Replica localReplica ;
                if(!offsetRequest.isFromDebuggingClient())
                    localReplica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic(), topicAndPartition.partition());
                else
                    localReplica = replicaManager.getReplicaOrException(topicAndPartition.topic(), topicAndPartition.partition());
                List<Long> offsets = new ArrayList<>();
                Long[] allOffsets = replicaManager.logManager.getOffsets(topicAndPartition,
                        partitionOffsetRequestInfo.time,
                        partitionOffsetRequestInfo.maxNumOffsets);
                if (!offsetRequest.isFromOrdinaryClient()) offsets = Arrays.asList(allOffsets);
                else {
                    boolean exists = false;
                    long hw = localReplica.highWatermark();
                    for(Long offset:allOffsets){
                        if(offset > hw){
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) offsets = Arrays.asList(allOffsets);
                }
                responseMap.put(topicAndPartition, new OffsetResponse.PartitionOffsetsResponse(ErrorMapping.NoError, offsets));
            } catch (UnknownTopicOrPartitionException utpe){
                // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
                // are typically transient and there is no value in logging the entire stack trace for the same
                logger.warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage()));
                responseMap.put(topicAndPartition,new OffsetResponse.PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass().getName()), null) );
            }catch (NotLeaderForPartitionException nle){
                logger.warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage()));
                responseMap.put(topicAndPartition, new OffsetResponse.PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass().getName()), null));
            }catch (Throwable e){
                logger.warn("Error while responding to offset request", e);
                responseMap.put(topicAndPartition, new OffsetResponse.PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass().getName()), null) );
            }
        }
        OffsetResponse response = new OffsetResponse(offsetRequest.correlationId, responseMap);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
    }

    /**
     * Service the topic metadata request API
     */
    public void handleTopicMetadataRequest(RequestChannel.Request request) throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException, InterruptedException {
        TopicMetadataRequest metadataRequest = (TopicMetadataRequest)request.requestObj;
        List<TopicMetadata> topicsMetadata = new ArrayList<>();
        KafkaConfig config = replicaManager.config;
        Set<String> uniqueTopics = new HashSet<>();
        if(metadataRequest.topics.size() > 0)
            uniqueTopics = metadataRequest.topics.stream().collect(Collectors.toSet());
        else
            uniqueTopics = ZkUtils.getAllTopics(zkClient).stream().collect(Collectors.toSet());
        List<TopicMetadata> topicMetadataList = new ArrayList<>();
        synchronized(partitionMetadataLock) {
            for(String topic:uniqueTopics){
                if(leaderCache.keySet().stream().map(x->x.topic()).collect(Collectors.toSet()).contains(topic)) {
                    Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> partitionStateInfo =  leaderCache.entrySet().stream().filter(p -> p.getKey().topic().equals(topic)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    Map<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> sortedPartitions = partitionStateInfo.entrySet().stream().sorted(Comparator.comparingInt(e -> e.getKey().partition()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
                    List<TopicMetadata.PartitionMetadata> partitionMetadata = new ArrayList<>();
                    for(Map.Entry<TopicAndPartition, LeaderAndIsrRequest.PartitionStateInfo> entry : sortedPartitions.entrySet()){
                        TopicAndPartition topicAndPartition = entry.getKey();
                        LeaderAndIsrRequest.PartitionStateInfo partitionState = entry.getValue();
                        Set<Integer> replicas = leaderCache.get(topicAndPartition).allReplicas;
                        List<Broker> replicaInfo = replicas.stream().map(r -> aliveBrokers.getOrDefault(r, null)).filter(r->r!= null).collect(Collectors.toList());
                        Broker leaderInfo = null;
                        List<Broker> isrInfo = new ArrayList<>();
                        LeaderIsrAndControllerEpoch leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch;
                        int leader = leaderIsrAndEpoch.leaderAndIsr.leader;
                        List<Integer> isr = leaderIsrAndEpoch.leaderAndIsr.isr;
                        logger.debug(String.format("%s",topicAndPartition.toString()) + ";replicas = " + replicas + ", in sync replicas = " + isr + ", leader = " + leader);
                        try {
                            if(aliveBrokers.keySet().contains(leader))
                                leaderInfo = aliveBrokers.get(leader);
                            else throw new LeaderNotAvailableException(String.format("Leader not available for partition %s",topicAndPartition.toString()));
                            isrInfo = isr.stream().map(i->aliveBrokers.getOrDefault(i, null)).filter(i->i != null).collect(Collectors.toList());
                            if(replicaInfo.size() < replicas.size()) {
                                List<Integer> replicaIds = replicaInfo.stream().map(r->r.id()).collect(Collectors.toList());
                                throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                                        replicas.stream().filter(r -> !replicaIds.contains(r)).collect(Collectors.toList()).toString());
                            }
                            if(isrInfo.size() < isr.size()) {
                                List<Integer> isrInfoIds = isrInfo.stream().map(r->r.id()).collect(Collectors.toList());
                                throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                                        isr.stream().filter(r -> !isrInfoIds.contains(r)).collect(Collectors.toList()).toString());
                            }
                            partitionMetadata.add(new TopicMetadata.PartitionMetadata(topicAndPartition.partition(), leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError));
                        } catch (Throwable e){
                            logger.error(String.format("Error while fetching metadata for partition %s",topicAndPartition.toString()), e);
                            partitionMetadata.add(new TopicMetadata.PartitionMetadata(topicAndPartition.partition(), leaderInfo, replicaInfo, isrInfo,
                                    ErrorMapping.codeFor(e.getClass().getName())));
                        }
                    }
                    topicMetadataList.add(new TopicMetadata(topic, partitionMetadata,ErrorMapping.NoError));
                } else {
                    // topic doesn't exist, send appropriate error code
                    topicMetadataList.add(new TopicMetadata(topic, new ArrayList<>(), ErrorMapping.UnknownTopicOrPartitionCode));
                }
            }
        }
        // handle auto create topics
        for(TopicMetadata topicMetadata:topicMetadataList){
            if(topicMetadata.errorCode == ErrorMapping.NoError ){
                topicsMetadata.add(topicMetadata);
            } else if (topicMetadata.errorCode == ErrorMapping.UnknownTopicOrPartitionCode) {
                if (config.autoCreateTopicsEnable) {
                    try {
                        CreateTopicCommand.createTopic(zkClient, topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor,"");
                        logger.info(String
                                .format("Auto creation of topic %s with %d partitions and replication factor %d is successful!",topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor));
                    } catch (TopicExistsException e){
                        // let it go, possibly another broker created this topic
                    }
                    topicsMetadata.add(new TopicMetadata(topicMetadata.topic, topicMetadata.partitionsMetadata, ErrorMapping.LeaderNotAvailableCode));
                } else {
                    topicsMetadata.add(topicMetadata);
                }
            }else {
                logger.debug(String.format("Error while fetching topic metadata for topic %s due to %s ",topicMetadata.topic,
                        ErrorMapping.exceptionFor(topicMetadata.errorCode).getClass().getName()));
                topicsMetadata.add(topicMetadata);
            }
        }
        logger.trace(String.format("Sending topic metadata %s for correlation id %d to client %s",topicsMetadata.toString(), metadataRequest.correlationId, metadataRequest.clientId));
        TopicMetadataResponse response = new TopicMetadataResponse(metadataRequest.correlationId,topicsMetadata);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
    }

    public void close() throws InterruptedException {
        logger.debug("Shutting down.");
        fetchRequestPurgatory.shutdown();
        producerRequestPurgatory.shutdown();
        logger.debug("Shut down complete.");
    }


    class RequestKey{
        String topic;
        int partition;

        public RequestKey(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        public RequestKey(TopicAndPartition topicAndPartition) {
            this(topicAndPartition.topic(), topicAndPartition.partition());
        }

        public TopicAndPartition topicAndPartition(){
            return new TopicAndPartition(topic, partition);
        }

        public String keyLabel(){
            return "%s-%d".format(topic, partition);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestKey that = (RequestKey) o;
            return partition == that.partition &&
                    Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }
    }

    /**
     * A delayed fetch request
     */
    class DelayedFetch extends DelayedRequest<RequestKey>{

        List<RequestKey> keys;
        RequestChannel.Request  request;
        FetchRequest fetch;
        long initialSize;

        public DelayedFetch(List<RequestKey> keys, RequestChannel.Request request, FetchRequest fetch,long delayMs, long initialSize) {
            super(keys, request, delayMs);
            this.request = request;
            this.fetch = fetch;
            this.initialSize = initialSize;
        }

        AtomicLong bytesAccumulated = new AtomicLong(initialSize);
    }


    /**
     * A holding pen for fetch requests waiting to be satisfied
     */
    class FetchRequestPurgatory extends RequestPurgatory<DelayedFetch, Integer> {

        RequestChannel requestChannel;
        int purgeInterval;

        public FetchRequestPurgatory( RequestChannel requestChannel, int purgeInterval) {
            super(KafkaApis.this.brokerId, purgeInterval);
            this.requestChannel = requestChannel;
            this.purgeInterval = purgeInterval;
        }

        /**
         * A fetch request is satisfied when it has accumulated enough data to meet the min_bytes field
         */
        public boolean checkSatisfied(Integer messageSizeInBytes,DelayedFetch delayedFetch) {
            long accumulatedSize = delayedFetch.bytesAccumulated.addAndGet(messageSizeInBytes);
            return accumulatedSize >= delayedFetch.fetch.minBytes;
        }

        /**
         * When a request expires just answer it with whatever data is present
         */
        public void expire(DelayedFetch delayed) throws UnsupportedEncodingException,InterruptedException{
            logger.debug("Expiring fetch request %s.".format(delayed.fetch.toString()));
            try {
                Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> topicData = readMessageSets(delayed.fetch);
                FetchResponse response = new FetchResponse(delayed.fetch.correlationId, topicData);
                requestChannel.sendResponse(new RequestChannel.Response(delayed.request, new FetchResponse.FetchResponseSend(response)));
            }
            catch (LeaderNotAvailableException e1){
                logger.debug("Leader changed before fetch request %s expired.".format(delayed.fetch.toString()));
            }catch (UnknownTopicOrPartitionException e2){
                logger.debug("Replica went offline before fetch request %s expired.".format(delayed.fetch.toString()));
            }
        }
    }

    class DelayedProduce extends DelayedRequest{

        List<RequestKey> keys;
        RequestChannel.Request request;
        Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus>  initialErrorsAndOffsets;
        ProducerRequest produce;
        long delayMs;

        public DelayedProduce(List<RequestKey> keys, RequestChannel.Request request, Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus> initialErrorsAndOffsets, ProducerRequest produce, long delayMs) {
            super(keys, request, delayMs);
            this.keys = keys;
            this.request = request;
            this.initialErrorsAndOffsets = initialErrorsAndOffsets;
            this.produce = produce;
            this.delayMs = delayMs;
            for(RequestKey requestKey:keys){
                ProducerResponse.ProducerResponseStatus producerResponseStatus = initialErrorsAndOffsets.get(new TopicAndPartition(requestKey.topic, requestKey.partition));
                // if there was an error in writing to the local replica's log, then don't
                // wait for acks on this partition
                boolean acksPending = false;
                short error = producerResponseStatus.error;
                long nextOffset = producerResponseStatus.offset;
                if (producerResponseStatus.error == ErrorMapping.NoError) {
                    // Timeout error state will be cleared when requiredAcks are received
                    acksPending = true;
                    error = ErrorMapping.RequestTimedOutCode;
                }
                PartitionStatus initialStatus = new PartitionStatus(acksPending, error, nextOffset);
                logger.trace("Initial partition status for %s = %s".format(requestKey.keyLabel(), initialStatus));
                partitionStatus.put(requestKey, initialStatus);
            }
        }
        Map<RequestKey,PartitionStatus> partitionStatus = new HashMap<>();


        public void respond() throws IOException, InterruptedException {
            Map<TopicAndPartition, ProducerResponse.ProducerResponseStatus> status = new HashMap<>();
            for(Map.Entry<TopicAndPartition, ProducerResponse.ProducerResponseStatus> entry : initialErrorsAndOffsets.entrySet()){
                PartitionStatus pstat = partitionStatus.get(new RequestKey(entry.getKey()));
                status.put(entry.getKey(),new ProducerResponse.ProducerResponseStatus(pstat.error, pstat.requiredOffset));
            }
            ProducerResponse response = new ProducerResponse(produce.correlationId, status);
            requestChannel.sendResponse(new RequestChannel.Response(
                    request, new BoundedByteBufferSend(response)));
        }

        /**
         * Returns true if this delayed produce request is satisfied (or more
         * accurately, unblocked) -- this is the case if for every partition:
         * Case A: This broker is not the leader: unblock - should return error.
         * Case B: This broker is the leader:
         *   B.1 - If there was a localError (when writing to the local log): unblock - should return error
         *   B.2 - else, at least requiredAcks replicas should be caught up to this request.
         *
         * As partitions become acknowledged, we may be able to unblock
         * DelayedFetchRequests that are pending on those partitions.
         */
        public boolean isSatisfied(RequestKey followerFetchRequestKey) throws Exception {
            String topic = followerFetchRequestKey.topic;
            int partitionId = followerFetchRequestKey.partition;
            RequestKey key = new RequestKey(topic, partitionId);
            PartitionStatus fetchPartitionStatus = partitionStatus.get(key);
            logger.trace("Checking producer request satisfaction for %s-%d, acksPending = %b"
                    .format(topic, partitionId, fetchPartitionStatus.acksPending));
            if (fetchPartitionStatus.acksPending) {
                Partition partition = replicaManager.getPartition(topic, partitionId);
                boolean hasEnough = false;
                short errorCode = ErrorMapping.UnknownTopicOrPartitionCode;
                if(partition != null){
                    Pair<Boolean, Short> pair = partition.checkEnoughReplicasReachOffset(fetchPartitionStatus.requiredOffset, produce.requiredAcks);
                    hasEnough = pair.getKey();
                    errorCode = pair.getValue();
                }
                if (errorCode != ErrorMapping.NoError) {
                    fetchPartitionStatus.acksPending = false;
                    fetchPartitionStatus.error = errorCode;
                } else if (hasEnough) {
                    fetchPartitionStatus.acksPending = false;
                    fetchPartitionStatus.error = ErrorMapping.NoError;
                }
                if (!fetchPartitionStatus.acksPending) {
                    Integer messageSizeInBytes = produce.topicPartitionMessageSizeMap.get(followerFetchRequestKey.topicAndPartition());
                    maybeUnblockDelayedFetchRequests(topic, partitionId, messageSizeInBytes);
                }
            }

            // unblocked if there are no partitions with pending acks
            boolean satisfied = false;
            for(Map.Entry<RequestKey,PartitionStatus> entry : partitionStatus.entrySet()){
                if(entry.getValue().acksPending){
                    satisfied = true;
                    break;
                }
            }
            logger.trace("Producer request satisfaction for %s-%d = %b".format(topic, partitionId, satisfied));
            return satisfied;
        }

    }

    class PartitionStatus{
        boolean acksPending;
        short error;
        long requiredOffset;

        public PartitionStatus(boolean acksPending, short error, long requiredOffset) {
            this.acksPending = acksPending;
            this.error = error;
            this.requiredOffset = requiredOffset;
        }

        public void setThisBrokerNotLeader() {
            error = ErrorMapping.NotLeaderForPartitionCode;
            acksPending = false;
        }

        @Override
        public String toString() {
            return "PartitionStatus{" +
                    "acksPending=" + acksPending +
                    ", error=" + error +
                    ", requiredOffset=" + requiredOffset +
                    '}';
        }
    }

    /**
     * A holding pen for produce requests waiting to be satisfied.
     */
    class ProducerRequestPurgatory
            extends RequestPurgatory<DelayedProduce, RequestKey> {

        public ProducerRequestPurgatory( int purgeInterval){
            super(KafkaApis.this.brokerId,purgeInterval);
        }

        protected boolean checkSatisfied(RequestKey followerFetchRequestKey,
                                         DelayedProduce delayedProduce) throws Exception {
            return delayedProduce.isSatisfied(followerFetchRequestKey);
        }

        /**
         * Handle an expired delayed request
         */
        protected void expire(DelayedProduce delayedProduce) throws Exception{
            delayedProduce.respond();
        }
    }
    public  class ProduceResult {

        public TopicAndPartition key;
        public long start;
        public long end;
        public Throwable error = null;

        public ProduceResult(TopicAndPartition key, long start, long end, Throwable error) {
            this.key = key;
            this.start = start;
            this.end = end;
            this.error = error;
        }

        public ProduceResult(TopicAndPartition key,Throwable throwable) {
            this(key, -1L, -1L,throwable);
        }

        public short errorCode (){
            if(error == null){
                return ErrorMapping.NoError;
            }
            return ErrorMapping.codeFor(error.getClass().getName());
        }
    }
}


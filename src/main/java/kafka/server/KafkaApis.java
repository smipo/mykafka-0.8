package kafka.server;

import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pair;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
    public void handle(RequestChannel.Request request) {
        try{
            logger.trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress);
            request.requestId match {
                case RequestKeys.ProduceKey => handleProducerRequest(request)
                case RequestKeys.FetchKey => handleFetchRequest(request)
                case RequestKeys.OffsetsKey => handleOffsetRequest(request)
                case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
                case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
                case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
                case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)
                case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
                case requestId => throw new KafkaException("No mapping found for handler id " + requestId)
            }
        } catch (Throwable e){
                request.requestObj.handleError(e, requestChannel, request);
                logger.error("error when handling request %s".format(request.requestObj+""), e);
        } finally {
            request.apiLocalCompleteTimeMs = SystemTime.milliseconds;
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
            String stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
                    "old controller %d with epoch %d. Latest known controller epoch is %d").format(brokerId+"",
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
                     logger.trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
                             "sent by controller %d epoch %d with correlation id %d").format(brokerId+"", entry.getValue().toString(), entry.getKey().toString(),
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
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(controlledShutdownResponse)))
    }

    /**
     * Check if a partitionData from a produce request can unblock any
     * DelayedFetch requests.
     */
    public void maybeUnblockDelayedFetchRequests(String topic ,int partition,int messageSizeInBytes) throws UnsupportedEncodingException, InterruptedException {
        List<DelayedFetch> satisfied =  fetchRequestPurgatory.update(new RequestKey(topic, partition), messageSizeInBytes);
        logger.trace("Producer request to (%s-%d) unblocked %d fetch requests.".format(topic, partition, satisfied.size()));

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
    public void handleProducerRequest(RequestChannel.Request request) throws IOException, InterruptedException {
        ProducerRequest produceRequest = (ProducerRequest)request.requestObj;
        long sTime = System.currentTimeMillis();
        List<ProduceResult> localProduceResults = appendToLocalLog(produceRequest);
        logger.debug("Produce to local log in %d ms".format((System.currentTimeMillis() - sTime)+""));
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
                logger.info(("Send the close connection response due to error handling produce request " +
                        "[clientId = %s, correlationId = %s, topicAndPartition = %s] with Ack=0")
                        .format(produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet().toString()));
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
        logger.trace("Append [%s] to local log ".format(partitionAndData.toString()));
        List<ProduceResult> res = new ArrayList<>();
        for(Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : partitionAndData.entrySet()){
            TopicAndPartition topicAndPartition = entry.getKey();
            ByteBufferMessageSet messages = entry.getValue();
            try {
                Partition partition = replicaManager.getPartition(topicAndPartition.topic(), topicAndPartition.partition());
                if(partition == null){
                    throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
                            .format(topicAndPartition.toString(), brokerId));
                }
                Pair<Long,Long> pair = partition.appendMessagesToLeader(messages);
                logger.trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
                        .format(messages.toString(), topicAndPartition.topic(), topicAndPartition.partition(), pair.getKey(), pair.getValue()));
                res.add(new ProduceResult(topicAndPartition, pair.getKey(), pair.getValue(),null));
            } catch (KafkaStorageException e){
                // NOTE: Failed produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
                // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request
                // for a partition it is the leader for
                    logger.fatal("Halting due to unrecoverable I/O error while handling produce request: ", e);
                    Runtime.getRuntime().halt(1);
            }catch (UnknownTopicOrPartitionException utpe){
                logger.warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
                        producerRequest.correlationId+"", producerRequest.clientId, topicAndPartition, utpe.getMessage()));
                res.add(new ProduceResult(topicAndPartition, utpe));
            }catch (NotLeaderForPartitionException nle){
                logger.warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
                        producerRequest.correlationId+"", producerRequest.clientId, topicAndPartition, nle.getMessage()));
                res.add(new ProduceResult(topicAndPartition, nle));
            }catch (Throwable e){
                logger.error("Error processing ProducerRequest with correlation id %d from client %s on partition %s"
                        .format(producerRequest.correlationId+"", producerRequest.clientId, topicAndPartition), e);
                res.add(new ProduceResult(topicAndPartition, e));
            }
        }
        return res;
    }

    /**
     * Handle a fetch request
     */
    public void handleFetchRequest(RequestChannel.Request request) throws IOException, InterruptedException {
        FetchRequest fetchRequest = (FetchRequest)request.requestObj;
        if(fetchRequest.isFromFollower()) {
            maybeUpdatePartitionHw(fetchRequest);
            // after updating HW, some delayed produce requests may be unblocked
            List<DelayedProduce> satisfiedProduceRequests = new ArrayList<>();
            for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
                RequestKey key = new RequestKey(entry.getKey());
                satisfiedProduceRequests.addAll(producerRequestPurgatory.update(key, key));
            }
            logger.debug("Replica %d fetch unblocked %d producer requests."
                    .format(fetchRequest.replicaId+"", satisfiedProduceRequests.size()));
            for(DelayedProduce d:satisfiedProduceRequests){
                d.respond();
            }
        }

        val dataRead = readMessageSets(fetchRequest);
        val bytesReadable = dataRead.values.map(_.messages.sizeInBytes).sum
        if(fetchRequest.maxWait <= 0 ||
                bytesReadable >= fetchRequest.minBytes ||
                fetchRequest.numPartitions <= 0) {
            debug("Returning fetch response %s for fetch request with correlation id %d to client %s"
                    .format(dataRead.values.map(_.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
            val response = new FetchResponse(fetchRequest.correlationId, dataRead)
            requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
        } else {
            debug("Putting fetch request with correlation id %d from client %s into purgatory".format(fetchRequest.correlationId,
                    fetchRequest.clientId))
            // create a list of (topic, partition) pairs to use as keys for this delayed request
            val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq.map(new RequestKey(_))
            val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest, fetchRequest.maxWait, bytesReadable)
            fetchRequestPurgatory.watch(delayedFetch)
        }
    }

    private void maybeUpdatePartitionHw(FetchRequest fetchRequest) throws IOException {
        logger.debug("Maybe update partition HW due to fetch request: %s ".format(fetchRequest.toString()));
        for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
            replicaManager.recordFollowerPosition(entry.getKey().topic(), entry.getKey().partition(), fetchRequest.replicaId, entry.getValue().offset);
        }
    }

    /**
     * Read from all the offset details given and return a map of
     * (topic, partition) -> PartitionData
     */
    private  Map<TopicAndPartition, FetchResponse.FetchResponsePartitionData> readMessageSets(FetchRequest fetchRequest)  {
        boolean isFetchFromFollower = fetchRequest.isFromFollower();
        for(Map.Entry<TopicAndPartition, FetchRequest.PartitionFetchInfo> entry : fetchRequest.requestInfo.entrySet()){
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            long offset = entry.getValue().offset;
            long fetchSize = entry.getValue().fetchSize;
            val partitionData =;
            try {
                val (messages, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId)
                BrokerTopicStats.getBrokerTopicStats(topic).bytesOutRate.mark(messages.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesOutRate.mark(messages.sizeInBytes)
                if (!isFetchFromFollower) {
                    new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
                } else {
                    debug("Leader %d for partition [%s,%d] received fetch request from follower %d"
                            .format(brokerId, topic, partition, fetchRequest.replicaId))
                    new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, messages)
                }
            } catch {
                // NOTE: Failed fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
                // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request
                // for a partition it is the leader for
                case utpe: UnknownTopicOrPartitionException =>
                    warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
                            fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage))
                    new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
                case nle: NotLeaderForPartitionException =>
                    warn("Fetch request with correlation id %d from client %s on partition [%s,%d] failed due to %s".format(
                            fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage))
                    new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
                case t: Throwable =>
                    BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
                    BrokerTopicStats.getBrokerAllTopicsStats.failedFetchRequestRate.mark()
                    error("Error when processing fetch request for partition [%s,%d] offset %d from %s with correlation id %d"
                            .format(topic, partition, offset, if (isFetchFromFollower) "follower" else "consumer", fetchRequest.correlationId), t)
                    new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf[Class[Throwable]]), -1L, MessageSet.Empty)
            }
            (TopicAndPartition(topic, partition), partitionData)
        }

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
            logger.error("Leader for partition [%s,%d] on broker %d does not have a local log".format(topic, partition, brokerId));
            messages = MessageSet.Empty;
        }
        return new Pair<>(messages, localReplica.highWatermark());
    }

    /**
     * Service the offset request API
     */
    public void handleOffsetRequest(RequestChannel.Request request) {
        OffsetRequest offsetRequest = (OffsetRequest)request.requestObj;
        val responseMap = offsetRequest.requestInfo.map(elem => {
                val (topicAndPartition, partitionOffsetRequestInfo) = elem
        try {
            // ensure leader exists
            val localReplica = if(!offsetRequest.isFromDebuggingClient)
                replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
            else
                replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition)
            val offsets = {
                    val allOffsets = replicaManager.logManager.getOffsets(topicAndPartition,
                    partitionOffsetRequestInfo.time,
                    partitionOffsetRequestInfo.maxNumOffsets)
            if (!offsetRequest.isFromOrdinaryClient) allOffsets
            else {
                val hw = localReplica.highWatermark
                if (allOffsets.exists(_ > hw))
                    hw +: allOffsets.dropWhile(_ > hw)
            else allOffsets
            }
        }
            (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
        } catch {
            // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
            // are typically transient and there is no value in logging the entire stack trace for the same
            case utpe: UnknownTopicOrPartitionException =>
                warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage))
                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
            case nle: NotLeaderForPartitionException =>
                warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage))
                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
            case e: Throwable =>
                warn("Error while responding to offset request", e)
                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        }
    })
        val response = OffsetResponse(offsetRequest.correlationId, responseMap)
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }

    /**
     * Service the topic metadata request API
     */
    public void handleTopicMetadataRequest(RequestChannel.Request request) {
        val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
        val topicsMetadata = new mutable.ArrayBuffer[TopicMetadata]()
        val config = replicaManager.config
        var uniqueTopics = Set.empty[String]
        uniqueTopics = {
        if(metadataRequest.topics.size > 0)
            metadataRequest.topics.toSet
        else
            ZkUtils.getAllTopics(zkClient).toSet
    }
        val topicMetadataList =
                partitionMetadataLock synchronized {
            uniqueTopics.map { topic =>
                if(leaderCache.keySet.map(_.topic).contains(topic)) {
                    val partitionStateInfo = leaderCache.filter(p => p._1.topic.equals(topic))
                    val sortedPartitions = partitionStateInfo.toList.sortWith((m1,m2) => m1._1.partition < m2._1.partition)
                    val partitionMetadata = sortedPartitions.map { case(topicAndPartition, partitionState) =>
                        val replicas = leaderCache(topicAndPartition).allReplicas
                        var replicaInfo: Seq[Broker] = replicas.map(aliveBrokers.getOrElse(_, null)).filter(_ != null).toSeq
                        var leaderInfo: Option[Broker] = None
                        var isrInfo: Seq[Broker] = Nil
                        val leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch
                        val leader = leaderIsrAndEpoch.leaderAndIsr.leader
                        val isr = leaderIsrAndEpoch.leaderAndIsr.isr
                        debug("%s".format(topicAndPartition) + ";replicas = " + replicas + ", in sync replicas = " + isr + ", leader = " + leader)
                        try {
                            if(aliveBrokers.keySet.contains(leader))
                                leaderInfo = Some(aliveBrokers(leader))
                            else throw new LeaderNotAvailableException("Leader not available for partition %s".format(topicAndPartition))
                            isrInfo = isr.map(aliveBrokers.getOrElse(_, null)).filter(_ != null)
                            if(replicaInfo.size < replicas.size)
                                throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                                        replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
                            if(isrInfo.size < isr.size)
                                throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                                        isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
                            new PartitionMetadata(topicAndPartition.partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
                        } catch {
                        case e: Throwable =>
                            error("Error while fetching metadata for partition %s".format(topicAndPartition), e)
                            new PartitionMetadata(topicAndPartition.partition, leaderInfo, replicaInfo, isrInfo,
                                    ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
                    }
                    }
                    new TopicMetadata(topic, partitionMetadata)
                } else {
                    // topic doesn't exist, send appropriate error code
                    new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
                }
            }
        }

        // handle auto create topics
        topicMetadataList.foreach { topicMetadata =>
            topicMetadata.errorCode match {
                case ErrorMapping.NoError => topicsMetadata += topicMetadata
                case ErrorMapping.UnknownTopicOrPartitionCode =>
                    if (config.autoCreateTopicsEnable) {
                        try {
                            CreateTopicCommand.createTopic(zkClient, topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor)
                            info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                                    .format(topicMetadata.topic, config.numPartitions, config.defaultReplicationFactor))
                        } catch {
                            case e: TopicExistsException => // let it go, possibly another broker created this topic
                        }
                        topicsMetadata += new TopicMetadata(topicMetadata.topic, topicMetadata.partitionsMetadata, ErrorMapping.LeaderNotAvailableCode)
                    } else {
                        topicsMetadata += topicMetadata
                    }
                case _ =>
                    debug("Error while fetching topic metadata for topic %s due to %s ".format(topicMetadata.topic,
                            ErrorMapping.exceptionFor(topicMetadata.errorCode).getClass.getName))
                    topicsMetadata += topicMetadata
            }
        }
        trace("Sending topic metadata %s for correlation id %d to client %s".format(topicsMetadata.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
        val response = new TopicMetadataResponse(topicsMetadata.toSeq, metadataRequest.correlationId)
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
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

        public DelayedFetch(long delayMs, List<RequestKey> keys, RequestChannel.Request request, FetchRequest fetch, long initialSize) {
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
        public boolean isSatisfied(RequestKey followerFetchRequestKey) {
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
                                     DelayedProduce delayedProduce) {
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


package kafka.server;

import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.common.MessageSizeTooLargeException;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.message.ByteBufferMessageSet;
import kafka.network.Handler;
import kafka.network.Receive;
import kafka.network.Send;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.helpers.LogLog.warn;

public class KafkaRequestHandlers implements Handler{

    private static Logger logger = Logger.getLogger(KafkaRequestHandlers.class);

    LogManager logManager;

    public KafkaRequestHandlers(LogManager logManager){
        this.logManager = logManager;
    }

    public Send handler(short requestTypeId, Receive request) throws Exception{
        if(requestTypeId == RequestKeys.Produce) return handleProducerRequest(request);
        else if(requestTypeId == RequestKeys.Fetch) return handleFetchRequest(request);
        else if(requestTypeId == RequestKeys.MultiFetch) return handleMultiFetchRequest(request);
        else if(requestTypeId == RequestKeys.MultiProduce) return handleMultiProducerRequest(request);
        else if(requestTypeId == RequestKeys.Offsets) return handleOffsetRequest(request);
        else throw new IllegalStateException("No mapping found for handler id " + requestTypeId);
    }

    public Send handleProducerRequest(Receive receive) throws Exception{
        long sTime = System.currentTimeMillis();
        ProducerRequest request = ProducerRequest.readFrom(receive.buffer());
        logger.info("Producer request " + request.toString());
        handleProducerRequest(request, "ProduceRequest");
        logger.info("kafka produce time " + (System.currentTimeMillis() - sTime) + " ms");
        return null;
    }

    public Send handleMultiProducerRequest(Receive receive) throws Exception{
        MultiProducerRequest request = MultiProducerRequest.readFrom(receive.buffer());
        logger.info("Multiproducer request " + request.toString());
        for(ProducerRequest producerRequest:request.produces()){
            handleProducerRequest(producerRequest, "MultiProducerRequest");
        }
        return null;
    }

    private void handleProducerRequest(ProducerRequest request, String requestHandlerName) throws Exception{
        int partition = request.partition();
        if (partition == -1){
            partition = logManager.chooseRandomPartition(request.topic());
        }
        try {
            logManager.getOrCreateLog(request.topic(), partition).append(request.messages());
            logger.info(request.messages().sizeInBytes() + " bytes written to logs.");
        }
        catch (MessageSizeTooLargeException e){
            logger.warn(e.getMessage() + " on " + request.topic() + ":" + partition);
        }catch (Exception e){
            logger.error("Error processing " + requestHandlerName + " on " + request.topic() + ":" + partition, e);
            throw  e;
        }
    }

    public Send handleFetchRequest(Receive request)  throws IOException{
        FetchRequest fetchRequest = FetchRequest.readFrom(request.buffer());
        logger.info("Fetch request " + fetchRequest.toString());
        return readMessageSet(fetchRequest);
    }

    public  Send handleMultiFetchRequest(Receive request) throws IOException{
        MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(request.buffer());
        logger.info("Multifetch request " + request.toString());
        List<MessageSetSend> responses = new ArrayList<>();
        for(FetchRequest fetch:multiFetchRequest.fetches()){
            responses.add(readMessageSet(fetch));
        }
        return new MultiMessageSetSend(responses);
    }

    private MessageSetSend readMessageSet(FetchRequest fetchRequest) {
        MessageSetSend  response = null;
        try {
            logger.info("Fetching log segment for topic, partition, offset, maxSize = " + fetchRequest);
            Log log = logManager.getLog(fetchRequest.topic(), fetchRequest.partition());
            if (log != null) {
                response = new MessageSetSend(log.read(fetchRequest.offset(), fetchRequest.maxSize()));
            }
            else
                response = new MessageSetSend();
        }
        catch (Exception e){
            logger.error("error when processing request " + fetchRequest, e);
            response = new MessageSetSend(new ByteBufferMessageSet(ByteBuffer.allocate(0)), ErrorMapping.codeFor(e.getClass().getName()));
        }
       return response;
    }

    public Send handleOffsetRequest(Receive request) throws IOException,InterruptedException {
        OffsetRequest offsetRequest = OffsetRequest.readFrom(request.buffer());
        logger.info("Offset request " + offsetRequest.toString());
        long[] offsets = logManager.getOffsets(offsetRequest);
        OffsetRequest.OffsetArraySend response = new OffsetRequest.OffsetArraySend(offsets);
        return response;
    }
}

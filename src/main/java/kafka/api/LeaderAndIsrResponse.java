package kafka.api;

import kafka.common.ErrorMapping;
import kafka.utils.Pair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LeaderAndIsrResponse extends RequestOrResponse{

    public static LeaderAndIsrResponse readFrom(ByteBuffer buffer) throws UnsupportedEncodingException{
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();
        Map<Pair<String, Integer>, Short> responseMap = new HashMap<>();
        for (int i = 0 ;i < numEntries;i++){
            String topic = ApiUtils.readShortString(buffer);
            int partition = buffer.getInt();
            short partitionErrorCode = buffer.getShort();
            responseMap.put(new Pair<>(topic, partition), partitionErrorCode);
        }
        return new LeaderAndIsrResponse(correlationId, responseMap, errorCode);
    }


    public Map<Pair<String, Integer>, Short> responseMap;
    public short errorCode = ErrorMapping.NoError;

    public LeaderAndIsrResponse(int correlationId,Map<Pair<String, Integer>, Short> responseMap, short errorCode) {
        super(null,correlationId);
        this.responseMap = responseMap;
        this.errorCode = errorCode;
    }

    public int sizeInBytes() throws IOException {
        int size =
                4 /* correlation id */ +
                        2 /* error code */ +
                        4; /* number of responses */
        for (Map.Entry<Pair<String, Integer>, Short> entry : responseMap.entrySet()) {
            size +=
                    2 + entry.getKey().getKey().length() /* topic */ +
                            4 /* partition */ +
                            2; /* error code for this partition */
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) throws IOException {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(responseMap.size());
        for (Map.Entry<Pair<String, Integer>, Short> entry : responseMap.entrySet()) {
            ApiUtils.writeShortString(buffer, entry.getKey().getKey());
            buffer.putInt(entry.getKey().getValue());
            buffer.putShort(entry.getValue());
        }
    }
}

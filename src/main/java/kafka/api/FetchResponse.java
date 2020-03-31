package kafka.api;

import kafka.common.ErrorMapping;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;

import java.nio.ByteBuffer;

public class FetchResponse {

    public static class FetchResponsePartitionData{


        public static FetchResponsePartitionData readFrom(ByteBuffer buffer) {
            short error = buffer.getShort();
            long hw = buffer.getLong();
            int messageSetSize = buffer.getInt();
            ByteBuffer messageSetBuffer = buffer.slice();
            messageSetBuffer.limit(messageSetSize);
            buffer.position(buffer.position() + messageSetSize);
            return new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer));
        }

        public static int headerSize =
                2 + /* error code */
                        8 + /* high watermark */
                        4 ;/* messageSetSize */

        public short error = ErrorMapping.NoError;
        public  long hw = -1L;
        public MessageSet messages;

        public FetchResponsePartitionData(short error, long hw, MessageSet messages) {
            this.error = error;
            this.hw = hw;
            this.messages = messages;
        }

        public int sizeInBytes (){
            return FetchResponsePartitionData.headerSize + messages.sizeInBytes();
        }
    }

    public static  class PartitionDataSend{
        public int partitionId;
        public FetchResponsePartitionData partitionData;

        public PartitionDataSend(int partitionId, FetchResponsePartitionData partitionData) {
            this.partitionId = partitionId;
            this.partitionData = partitionData;
        }
    }
}

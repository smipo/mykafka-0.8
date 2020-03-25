package kafka.consumer;

import kafka.message.ByteBufferMessageSet;

import java.util.Objects;

public class FetchedDataChunk {

    ByteBufferMessageSet messages;
    PartitionTopicInfo topicInfo;
    long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }

    public ByteBufferMessageSet messages() {
        return messages;
    }

    public PartitionTopicInfo topicInfo() {
        return topicInfo;
    }

    public long fetchOffset() {
        return fetchOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchedDataChunk that = (FetchedDataChunk) o;
        return fetchOffset == that.fetchOffset &&
                Objects.equals(messages, that.messages) &&
                Objects.equals(topicInfo, that.topicInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messages, topicInfo, fetchOffset);
    }
}

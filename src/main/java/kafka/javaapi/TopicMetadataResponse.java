package kafka.javaapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopicMetadataResponse {

    kafka.api.TopicMetadataResponse underlying;

    public TopicMetadataResponse(kafka.api.TopicMetadataResponse underlying) {
        this.underlying = underlying;
    }
    public int sizeInBytes() throws IOException {
        return underlying.sizeInBytes();
    }

    public List<TopicMetadata> topicsMetadata() {
        List<TopicMetadata> list = new ArrayList<>();
        for(kafka.api.TopicMetadata t:underlying.topicsMetadata){
            list.add(new TopicMetadata(t));
        }
        return list;
    }
}

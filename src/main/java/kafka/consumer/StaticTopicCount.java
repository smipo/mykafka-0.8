package kafka.consumer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StaticTopicCount extends TopicCount{

    String consumerIdString;
    Map<String, Integer> topicCountMap;

    public StaticTopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        return  makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }
    /**
     *  return json of
     *  { "topic1" : 4,
     *    "topic2" : 4
     *  }
     */
    public  String dbString(){
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int i = 0;
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            String topic = entry.getKey();
            Integer nConsumers = entry.getValue();
            if (i > 0)
                builder.append(",");
            builder.append("\""+topic+"\"" + ":" + nConsumers);
            i += 1;
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StaticTopicCount that = (StaticTopicCount) o;
        return Objects.equals(consumerIdString, that.consumerIdString) &&
                Objects.equals(topicCountMap, that.topicCountMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerIdString, topicCountMap);
    }
}

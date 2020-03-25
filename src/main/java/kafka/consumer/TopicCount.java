package kafka.consumer;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class TopicCount {

    /*
     * Example of whitelist topic count stored in ZooKeeper:
     * Topics with whitetopic as prefix, and four streams: *4*whitetopic.*
     *
     * Example of blacklist topic count stored in ZooKeeper:
     * Topics with blacktopic as prefix, and four streams: !4!blacktopic.*
     */

    private static Logger logger = Logger.getLogger(TopicCount.class);

    public  static String WHITELIST_MARKER = "*";
    public  static String BLACKLIST_MARKER = "!";
    public  static Pattern WHITELIST_PATTERN =
            Pattern.compile("\\*(\\p{Digit}+)\\*(.*)");
    public  static Pattern BLACKLIST_PATTERN =
            Pattern.compile("!(\\p{Digit}+)!(.*)");


    public abstract Map<String, Set<String>> getConsumerThreadIdsPerTopic();

    public abstract String dbString();

    protected Map<String, Set<String>> makeConsumerThreadIdsPerTopic(String consumerIdString,
                                                                     Map<String, Integer> topicCountMap)  {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = new HashMap<>();
        for(Map.Entry<String, Integer> entry : topicCountMap.entrySet()){
            String topic = entry.getKey();
            Integer nConsumers = entry.getValue();
            Set<String> consumerSet = new HashSet<>();
            assert(nConsumers >= 1);
            for (int i = 0 ;i < nConsumers; i++)
                consumerSet .add(consumerIdString + "-" + i);
            consumerThreadIdsPerTopicMap.put(topic, consumerSet);
        }
       return consumerThreadIdsPerTopicMap;
    }

}

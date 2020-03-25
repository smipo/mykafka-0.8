package kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static kafka.consumer.TopicCount.WHITELIST_MARKER;
import static kafka.consumer.TopicCount.BLACKLIST_MARKER;
import static kafka.consumer.TopicCount.BLACKLIST_PATTERN;
import static kafka.consumer.TopicCount.WHITELIST_PATTERN;
import static kafka.utils.Preconditions.checkArgument;

public class TopicCountFactory {

    private static Logger logger = Logger.getLogger(TopicCountFactory.class);

    private static ObjectMapper mapper = new ObjectMapper();

    public static TopicCount  constructTopicCount(String group,
                                                  String  consumerId,
                                                  ZkClient zkClient) {
        ZkUtils.ZKGroupDirs dirs = new ZkUtils.ZKGroupDirs(group);
        String topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId);
        boolean hasWhitelist = topicCountString.startsWith(WHITELIST_MARKER);
        boolean hasBlacklist = topicCountString.startsWith(BLACKLIST_MARKER);

        if (hasWhitelist || hasBlacklist) {
            Matcher matcher = null;
            if (hasWhitelist)
                matcher =  WHITELIST_PATTERN.matcher(topicCountString);
            else
                matcher =  BLACKLIST_PATTERN.matcher(topicCountString);
            checkArgument(matcher.matches());
            int numStreams = Integer.parseInt(matcher.group(1));
            String regex = matcher.group(2);
            TopicFilter filter = null;
            if (hasWhitelist)
                filter =  new Whitelist(regex);
            else
                filter = new Blacklist(regex);

           return new WildcardTopicCount(zkClient, consumerId, filter, numStreams);
        }
        else {
            Map<String, Integer> topMap  = new HashMap<>();
            try {
                if(topicCountString == null || topicCountString.isEmpty()) throw new RuntimeException("error constructing TopicCount : " + topicCountString);
                topMap = mapper.readValue(topicCountString, Map.class);
            }
            catch (Exception e){
                logger.error("error parsing consumer json string " + topicCountString, e);
                throw new RuntimeException(e.getMessage());
            }
           return new StaticTopicCount(consumerId, topMap);
        }
    }

    public static StaticTopicCount constructTopicCount(String consumerIdString,Map<String, Integer> topicCount) {
        return new StaticTopicCount(consumerIdString, topicCount);
    }

    public static WildcardTopicCount constructTopicCount(String consumerIdString,
                                                         TopicFilter filter,
                                                         int numStreams,
                                                         ZkClient zkClient) {
        return  new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams);
    }

}

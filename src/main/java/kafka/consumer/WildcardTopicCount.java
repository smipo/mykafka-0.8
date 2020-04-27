package kafka.consumer;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WildcardTopicCount  extends TopicCount {

    ZkClient zkClient;
    String consumerIdString;
    TopicFilter topicFilter;
    int numStreams;

    public WildcardTopicCount(ZkClient zkClient, String consumerIdString, TopicFilter topicFilter, int numStreams) {
        this.zkClient = zkClient;
        this.consumerIdString = consumerIdString;
        this.topicFilter = topicFilter;
        this.numStreams = numStreams;
    }

    public  Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        List<String> wildcardTopics = ZkUtils.getChildrenParentMayNotExist(
                zkClient, ZkUtils.BrokerTopicsPath).stream().filter(topic -> topicFilter.isTopicAllowed(topic)).collect(Collectors.toList());
        Map<String, Integer> topicCountMap  = new HashMap<>();
        for(String s:wildcardTopics){
            topicCountMap.put(s,numStreams);
        }
        return makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }

    public  String dbString(){
        String marker = TopicCount.BLACKLIST_MARKER;
        if(topicFilter instanceof  Whitelist){
            marker = TopicCount.WHITELIST_MARKER;
        }
        return String.format("%s%d%s%s",marker, numStreams, marker, topicFilter.regex);
    }
}

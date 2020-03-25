package kafka.consumer;

import org.apache.log4j.Logger;

public class Blacklist  extends TopicFilter{

    private static Logger logger = Logger.getLogger(Blacklist.class);

    public Blacklist(String rawRegex) {
        super(rawRegex);
    }

    public  boolean requiresTopicEventWatcher(){
        return true;
    }

    public  boolean isTopicAllowed(String topic) {
        boolean allowed = topic.matches(regex);
        String input = "filtered";
        if (allowed) input = "allowed";
        logger.debug("%s %s".format(
                topic,input ));

        return allowed;
    }
}

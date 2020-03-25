package kafka.consumer;

import org.apache.log4j.Logger;

public class Whitelist extends TopicFilter {

    private static Logger logger = Logger.getLogger(Whitelist.class);

    public Whitelist(String rawRegex) {
        super(rawRegex);
    }

    public  boolean requiresTopicEventWatcher(){
        return !regex.matches("[\\p{Alnum}-|]+");
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

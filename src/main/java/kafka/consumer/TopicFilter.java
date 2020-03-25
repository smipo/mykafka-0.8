package kafka.consumer;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class TopicFilter {

    String rawRegex;

    public TopicFilter(String rawRegex) {
        this.rawRegex = rawRegex;

        try {
            Pattern.compile(regex);
        }
        catch(PatternSyntaxException e) {
            throw new RuntimeException(regex + " is an invalid regex.");
        }
        regex = rawRegex
                .trim()
                .replace(',', '|')
                .replace(" ", "")
                .replaceAll("^[']+","")
                .replaceAll("[']+$","") ;
    }

    String regex ;

    public String toString (){
        return regex;
    }

    public abstract boolean requiresTopicEventWatcher();

    public abstract boolean isTopicAllowed(String topic);
}

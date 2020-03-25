package kafka.utils;

import kafka.common.InvalidTopicException;
import kafka.server.KafkaConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicNameValidator {

    KafkaConfig config;

    public TopicNameValidator(KafkaConfig config){
        this.config = config;
    }

    private String illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' +
            '\uD800' + "-" + '\uF8FF' + '\uFFF0' + "-" + '\uFFFF';
    //Todo
    // Regex checks for illegal chars and "." and ".." filenames
    //private String rgx = "(^\\.{1,2}$)|[" + illegalChars + "]";

    private String rgx = "(.{1,2}$)|[" + illegalChars + "]";

    Pattern pattern = Pattern.compile(rgx);

    public void validate(String topic) {
        if (topic.length() <= 0)
            throw new InvalidTopicException("topic name is illegal, can't be empty");
        else if (topic.length() > config.maxTopicNameLength)
            throw new InvalidTopicException("topic name is illegal, can't be longer than " + config.maxTopicNameLength + " characters");
        //Todo...
        Matcher m = pattern.matcher(topic);
        if(m.matches()){
            throw new InvalidTopicException("topic name " + topic + " is illegal, doesn't match expected regular expression");
        }
    }
}

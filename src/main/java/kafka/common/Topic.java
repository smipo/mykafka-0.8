package kafka.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Topic {

    private static String legalChars = "[a-zA-Z0-9\\._\\-]";
    private static int maxNameLength = 255;
    private static Pattern p = Pattern.compile(legalChars + "+");

    public static void validate(String topic) {
        if (topic.length() <= 0)
            throw new InvalidTopicException("topic name is illegal, can't be empty");
        else if (topic.equals(".") || topic.equals(".."))
            throw new InvalidTopicException("topic name cannot be \".\" or \"..\"");
        else if (topic.length() > maxNameLength)
            throw new InvalidTopicException("topic name is illegal, can't be longer than " + maxNameLength + " characters");
        Matcher m = p.matcher(topic);
        if(!m.find()){
            throw new InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }
}

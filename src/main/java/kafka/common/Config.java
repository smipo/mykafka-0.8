package kafka.common;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Config {

    public static void validateChars(String prop,String value) {
        String legalChars = "[a-zA-Z0-9\\._\\-]";
        Pattern rgx = Pattern.compile(legalChars + "*");
        Matcher matcher = rgx.matcher(value);
        if (matcher.find()){
            String res = matcher.group();
            if (!res.equals(value))
                throw new InvalidConfigException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }else{
            throw new InvalidConfigException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }
}

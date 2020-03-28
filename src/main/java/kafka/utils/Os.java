package kafka.utils;

public class Os {

    public static boolean isWindows(){
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }
}

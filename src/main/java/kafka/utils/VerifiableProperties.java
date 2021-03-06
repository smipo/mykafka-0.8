package kafka.utils;


import org.apache.log4j.Logger;

import java.util.*;

import static java.awt.SystemColor.info;
import static org.apache.log4j.helpers.LogLog.warn;

public class VerifiableProperties {

    private static Logger logger = Logger.getLogger(VerifiableProperties.class);

    public Properties props;

    public VerifiableProperties(Properties props) {
        this.props = props;
    }
    public VerifiableProperties(){
        this(new Properties());
    }
    private Set<String> referenceSet = new HashSet<>();


    public boolean containsKey(String name) {
        return props.containsKey(name);
    }

    public String getProperty(String name) {
        String value = props.get(name).toString();
        referenceSet.add(name);
        return value;
    }

    public  int  getInt(String name) {
        if(containsKey(name))
            return getInt(name, -1);
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
    public  int getInt(String name, int defaultValue){
        return getIntInRange(name, defaultValue,Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
    public  int getIntInRange(String name,int start,int end){
        return getIntInRange(name, -1,start, end);
    }
    public  int getIntInRange(String name, int defaultValue, int start,int end){
        int v = -1;
        if(containsKey(name))
            v = Integer.parseInt(getProperty(name));
        else
            v = defaultValue;

        if(v < start || v > end)
            throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start +","+ end + ".");
        return v;
    }


    public  long getLong(String name) {
        if(containsKey(name))
            return getLong( name, -1);
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }


    public  long getLong(String name, long defaultValue){
        return  getLongInRange( name, defaultValue, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public  long getLongInRange(String name, long defaultValue, long start,long end) {
        long v = -1;
        if(containsKey(name))
            v = Long.parseLong(getProperty(name));
        else
            v = defaultValue;

        if(v < start || v > end)
            throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start +","+ end + ".");
        return v;
    }
    /**
     * Get a string property, or, if no such property is defined, return the given default value
     */
    public  String getString(String name, String defaultValue) {
        if(containsKey(name))
            return getProperty(name);
        return defaultValue;
    }

    /**
     * Get a string property or throw and exception if no such property is defined.
     */
    public  String getString(String name) {
        if(containsKey(name))
            return getProperty(name);
        else
            throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    /**
     * Read a boolean value from the properties instance
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @return the boolean value
     */
    public  boolean getBoolean(String name,boolean defaultValue) {
        if(!containsKey(name))
            return defaultValue;
        else if("true".equals(props.get(name) == null?null:props.get(name).toString()))
            return true;
        else if("false".equals(props.get(name) == null?null:props.get(name).toString()))
            return false;
        else
            throw new IllegalArgumentException("Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false" );
    }
    /**
     * Get a Map[String, String] from a property list in the form k1:v2, k2:v2, ...
     */
    public Map<String,String> getMap(String name) {
        try {
            Map<String,String> m = Utils.parseCsvMap(getString(name, ""));
            for(Map.Entry<String, String> entry:m.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();
                if(Integer.parseInt(value) <= 0)
                    throw new IllegalArgumentException(String.format("Invalid entry '%s' = '%s' for property '%s'",key, value, name));

            }
            return m;
        } catch (Exception e){
            throw new IllegalArgumentException(String.format("Error parsing configuration property '%s': %s",name, e.getMessage()));
        }
    }
    public void verify() {
        logger.info("Verifying properties");
        Enumeration specifiedProperties = props.propertyNames();
        while (specifiedProperties.hasMoreElements()) {
            String key = specifiedProperties.nextElement().toString();
            if (!referenceSet.contains(key))
                logger.warn(String.format("Property %s is not valid",key));
            else
                logger.info(String.format("Property %s is overridden to %s",key, props.getProperty(key)));
        }
    }

    public short getShort(String name,Short defaultValue){
        return getShortInRange(name, defaultValue, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    public short getShortInRange(String name,Short defaultValue, short s,short e) {
        if(containsKey(name))
            return Short.parseShort(getProperty(name));
        else
            return  defaultValue;
    }

}

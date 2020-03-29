package kafka.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;


public class JacksonUtils {

    private static ObjectMapper mapper = new ObjectMapper();

    public static String objToJson(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    public static Map<String,Object> strToMap(String str) throws JsonProcessingException, JsonMappingException {
        return  mapper.readValue(str, Map.class);
    }
}

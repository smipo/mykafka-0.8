package kafka.utils;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;

public  class ZKStringSerializer implements ZkSerializer {

    private static Logger logger = Logger.getLogger(ZKStringSerializer.class);

    public ZKStringSerializer(){

    }

    public byte[] serialize(Object data) throws ZkMarshallingError {
        try{
            return String.valueOf(data).getBytes("UTF-8");
        }catch (UnsupportedEncodingException e){
            logger.error("zk serialize Error:",e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public  Object deserialize(byte[] bytes) throws ZkMarshallingError{
        if (bytes == null)
            return null;
        else
            try{
                return new String(bytes, "UTF-8");
            }catch (UnsupportedEncodingException e){
                logger.error("zk deserialize Error:",e);
                throw new RuntimeException(e.getMessage());
            }

    }
}


package kafka.serializer;

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;


public class StringDecoder implements Decoder<String> {

    VerifiableProperties props;

    public StringDecoder(VerifiableProperties props) {
        this.props = props;
        if(props == null)
            encoding =  "UTF8";
        else
            encoding =   props.getString("serializer.encoding", "UTF8");

    }

    String encoding ;

    public String fromBytes(byte[] bytes){
        try{
            return  new String(bytes, encoding);
        }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e.getMessage());
        }

    }
}

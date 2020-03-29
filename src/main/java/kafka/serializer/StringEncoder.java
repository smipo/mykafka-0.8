package kafka.serializer;

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

public class StringEncoder implements  Encoder<String> {


    VerifiableProperties props;

    public StringEncoder(VerifiableProperties props) {
        this.props = props;
        if(props == null)
            encoding =  "UTF8";
        else
            encoding =   props.getString("serializer.encoding", "UTF8");

    }

    String encoding ;

    public byte[] toBytes(String s){
        try{
            if(s == null)
                return null;
            else
                return s.getBytes(encoding);
        }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e.getMessage());
        }
    }
}

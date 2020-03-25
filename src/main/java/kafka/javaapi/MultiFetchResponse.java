package kafka.javaapi;

import kafka.message.ByteBufferMessageSet;
import kafka.utils.IteratorTemplate;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet>{

    ByteBuffer buffer;
    int numSets;
    long[] offsets;

    public MultiFetchResponse(ByteBuffer buffer, int numSets, long[] offsets) {
        this.buffer = buffer;
        this.numSets = numSets;
        this.offsets = offsets;

        underlyingBuffer = ByteBuffer.wrap(buffer.array());
        errorCode = underlyingBuffer.getShort();

        underlying = new kafka.api.MultiFetchResponse(underlyingBuffer, numSets, offsets);
    }

    ByteBuffer underlyingBuffer ;
    // this has the side effect of setting the initial position of buffer correctly
    short errorCode;

    kafka.api.MultiFetchResponse underlying ;

    @Override
    public String toString() {
        return underlying.toString();
    }

    public Iterator<ByteBufferMessageSet> iterator(){
        return  new IteratorTemplate<ByteBufferMessageSet>() {
            Iterator<ByteBufferMessageSet> iter = underlying.iterator();
            public ByteBufferMessageSet makeNext() {
                if(iter.hasNext())
                    return iter.next();
                else
                    return allDone();
            }
        };
    }
}

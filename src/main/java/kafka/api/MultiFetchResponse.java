package kafka.api;

import kafka.message.ByteBufferMessageSet;
import kafka.utils.IteratorTemplate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet>{

    ByteBuffer buffer;
    int numSets;
    long[] offsets;

    private List<ByteBufferMessageSet> messageSets = new LinkedList<>();

    public MultiFetchResponse( ByteBuffer buffer,
                               int numSets,
                               long[] offsets){
        this.buffer = buffer;
        this.numSets = numSets;
        this.offsets = offsets;

        for(int i = 0;i < numSets;i++) {
            int size = buffer.getInt();
            short errorCode = buffer.getShort();
            ByteBuffer copy = buffer.slice();
            int payloadSize = size - 2;
            copy.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            this.messageSets.add(new ByteBufferMessageSet(copy, offsets[i], errorCode));
        }
    }

    public Iterator<ByteBufferMessageSet> iterator()  {

        return new IteratorTemplate<ByteBufferMessageSet>() {

            Iterator<ByteBufferMessageSet> iter = messageSets.iterator();

            public ByteBufferMessageSet makeNext()  {
                if(iter.hasNext())
                    return iter.next();
                else
                    return allDone();
            }
        };
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public int numSets() {
        return numSets;
    }

    public long[] offsets() {
        return offsets;
    }

    public List<ByteBufferMessageSet> messageSets() {
        return messageSets;
    }
}

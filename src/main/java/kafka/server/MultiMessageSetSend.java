package kafka.server;

import kafka.network.ByteBufferSend;
import kafka.network.MultiSend;
import kafka.network.Send;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MultiMessageSetSend  extends MultiSend {


    public MultiMessageSetSend(List<MessageSetSend> sets){
        super(getSends(sets));
        //todo
        int allMessageSetSize = 0;
        for(MessageSetSend set:sets){
            allMessageSetSize += set.sendSize();
        }
        this.expectedBytesToWrite = 4 + 2 + allMessageSetSize;
    }

    public static List<Send> getSends(List<MessageSetSend> sets){
        //todo
        int allMessageSetSize = 0;
        for(MessageSetSend set:sets){
            allMessageSetSize += set.sendSize();
        }
        List<Send> sends = new ArrayList<>();
        ByteBufferSend byteBufferSend = new ByteBufferSend(6);
        ByteBuffer buffer = byteBufferSend.buffer();
        buffer.putInt(2 + allMessageSetSize);
        buffer.putShort((short) 0);
        buffer.rewind();
        sends.add(byteBufferSend);
        sends.addAll(sets);
        return sends;
    }
}

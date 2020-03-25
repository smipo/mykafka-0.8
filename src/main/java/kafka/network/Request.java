package kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Request {

    private short id;

    public Request(short id){
        this.id = id;
    }

    public abstract int sizeInBytes();

    public abstract void writeTo(ByteBuffer buffer) throws IOException;

    public short id() {
        return id;
    }
}

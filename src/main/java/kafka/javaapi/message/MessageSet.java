package kafka.javaapi.message;

import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 */
public abstract class MessageSet implements Iterable<MessageAndOffset>{

    /**
     * Provides an iterator over the messages in this set
     */
    public abstract java.util.Iterator<MessageAndOffset> iterator();

    /**
     * Gives the total size of this message set in bytes
     */
    public abstract long sizeInBytes();

    /**
     * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
     * match the payload for any message.
     */
    public void validate() {
        java.util.Iterator<MessageAndOffset> thisIterator = this.iterator();
        while(thisIterator.hasNext()) {
            MessageAndOffset messageAndOffset = thisIterator.next();
            if(!messageAndOffset.message().isValid())
                throw new InvalidMessageException();
        }
    }
}

package kafka.server;

import kafka.network.RequestChannel;
import kafka.utils.DelayedItem;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A request whose processing needs to be delayed for at most the given delayMs
 * The associated keys are used for bookeeping, and represent the "trigger" that causes this request to check if it is satisfied,
 * for example a key could be a (topic, partition) pair.
 */
public class DelayedRequest extends DelayedItem<RequestChannel.Request> {

    public List<Object> keys;

    public DelayedRequest(List<Object> keys,RequestChannel.Request item, long delayMs) {
        super(item, delayMs);
        this.keys = keys;
    }
    public AtomicBoolean satisfied = new AtomicBoolean(false);

}

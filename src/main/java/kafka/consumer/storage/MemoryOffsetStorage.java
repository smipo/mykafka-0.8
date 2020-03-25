package kafka.consumer.storage;

import kafka.utils.Pair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryOffsetStorage implements OffsetStorage {

    ConcurrentHashMap<Pair<Integer,String>,Pair<AtomicLong, Lock>> offsetAndLock = new ConcurrentHashMap<>();

    public long reserve(int node, String topic){
        Pair<Integer,String> key = new Pair<>(node, topic);
        if(!offsetAndLock.containsKey(key))
            offsetAndLock.putIfAbsent(key, new Pair(new AtomicLong(0), new ReentrantLock()));
        Pair<AtomicLong, Lock> value = offsetAndLock.get(key);
        value.getValue().lock();
        return value.getKey().get();
    }

    public void commit(int node, String topic,long offset) {
        Pair<Integer,String> key = new Pair<>(node, topic);
        Pair<AtomicLong, Lock> value = offsetAndLock.get(key);
        value.getKey().set(offset);
        value.getValue().unlock();
    }

}

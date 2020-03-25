package kafka.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Pool<K,V> {

    private ConcurrentHashMap<K,V> pool = new ConcurrentHashMap<K,V>();

    public Pool(){

    }
    public Pool(Map<K,V> m){
        for (Map.Entry<K, V> entry : m.entrySet()) {
            K k = entry.getKey();
            V v = entry.getValue();
            pool.put(k, v);
        }
    }
    public V put(K k, V v) {
        return pool.put(k, v);
    }
    public V putIfNotExists(K k, V v){
        return pool.putIfAbsent(k, v);
    }

    public boolean contains(K id) {
        return pool.containsKey(id);
    }

    public V get(K key){
        return pool.get(key);
    }

    public V remove(K key){
        return  pool.remove(key);
    }

    public ConcurrentHashMap.KeySetView<K,V> keys() {
        return pool.keySet();
    }

    public Collection<V> values(){
        return pool.values();
    }

    public void clear(){
        pool.clear();
    }

   public int size (){
        return pool.size();
   }

    public ConcurrentHashMap<K, V> pool() {
        return pool;
    }
}

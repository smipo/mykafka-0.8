package kafka.log;


import kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A copy-on-write list implementation that provides consistent views. The view() method
 * provides an immutable sequence representing a consistent state of the list. The user can do
 * iterative operations on this sequence such as binary search without locking all access to the list.
 * Even if the range of the underlying list changes no change will be made to the view
 */
public class SegmentList<T> {


    private AtomicReference<List<T>> contents = new AtomicReference();

    public SegmentList(List<T> logSegments){
        if(logSegments != null && logSegments.size() > 0){
            contents.set(logSegments);
        }
    }

    /**
     * Append the given items to the end of the list
     */
    public void append(T... ts) {
        List<T> curr = contents.get();
        for(int i = 0;i < ts.length;i++)
            curr.add(ts[i]);
    }
    /**
     * Delete the first n items from the list
     */
    public  List<T> trunc(int newStart) {
        if(newStart < 0)
            throw new KafkaException("Starting index must be positive.");
        List<T> curr = contents.get();
        if (curr.size() > 0) {
            int newLength = Math.max(curr.size() - newStart, 0);
            List<T> updated = new ArrayList<>();
            System.arraycopy(curr, Math.min(newStart, curr.size() - 1), updated, 0, newLength);
            contents.set(updated);
            List<T> deleted  = new ArrayList<>();
            System.arraycopy(curr, 0, deleted, 0, curr.size() - newLength);
            return deleted;
        }
        return null;
    }

    /**
     * Delete the items from position (newEnd + 1) until end of list
     */
    public List<T> truncLast(int newEnd) {
        if (newEnd < 0 || newEnd >= contents.get().size())
            throw new KafkaException("Attempt to truncate segment list of length %d to %d.".format(String.valueOf(contents.get().size()), newEnd));
        List<T> curr = contents.get();
        if (curr.size() > 0) {
            int newLength = newEnd + 1;
            List<T> updated = new ArrayList<>();
            System.arraycopy(curr, 0, updated, 0, newLength);
            contents.set(updated);
            List<T> deleted  = new ArrayList<>();
            System.arraycopy(curr, Math.min(newEnd + 1, curr.size() - 1), deleted, 0, curr.size() - newLength);
            return deleted;
        }
        return null;
    }

    /**
     * Get a consistent view of the sequence
     */
    public  List<T> view(){
        if(contents.get() == null) return null;
        return contents.get();
    }

    public T last(){
        List<T> curr = contents.get();
        if(curr == null) return null;
        return curr.get(curr.size() - 1);
    }
    /**
     * Nicer toString method
     */
    public String toString(){
        return "SegmentList(%s)".format(view().toString());
    }

}

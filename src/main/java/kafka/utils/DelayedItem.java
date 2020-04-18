package kafka.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedItem<T> implements Delayed {

    public  T item;
    public long delay;
    public TimeUnit unit;

    public DelayedItem(T item, long delay, TimeUnit unit) {
        this.item = item;
        this.delay = delay;
        this.unit = unit;

        long given = unit.toMillis(delay);
        if (given < 0 || (createdMs + given) < 0) delayMs = Long.MAX_VALUE - createdMs;
        else delayMs = given;
    }
    public DelayedItem(T item, long delayMs) {
        this(item, delayMs, TimeUnit.MILLISECONDS);
    }
    long createdMs = System.currentTimeMillis();
    long delayMs;


    /**
     * The remaining delay time
     */
    @Override
    public long getDelay(TimeUnit unit) {
        long elapsedMs = (System.currentTimeMillis() - createdMs);
        return unit.convert(Math.max(delayMs - elapsedMs, 0), TimeUnit.MILLISECONDS);
    }


    @Override
    public int compareTo(Delayed d) {
        DelayedItem<T> delayed = (DelayedItem<T>)d;
        long myEnd = createdMs + delayMs;
        long yourEnd = delayed.createdMs + delayed.delayMs;

        if(myEnd < yourEnd) return -1;
        else if(myEnd > yourEnd)return 1;
        else return 0;
    }
}

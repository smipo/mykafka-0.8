package kafka.server;

import kafka.utils.Pool;
import kafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A helper class for dealing with asynchronous requests with a timeout. A DelayedRequest has a request to delay
 * and also a list of keys that can trigger the action. Implementations can add customized logic to control what it means for a given
 * request to be satisfied. For example it could be that we are waiting for user-specified number of acks on a given (topic, partition)
 * to be able to respond to a request or it could be that we are waiting for a given number of bytes to accumulate on a given request
 * to be able to respond to that request (in the simple case we might wait for at least one byte to avoid busy waiting).
 *
 * For us the key is generally a (topic, partition) pair.
 * By calling
 *   watch(delayedRequest)
 * we will add triggers for each of the given keys. It is up to the user to then call
 *   val satisfied = update(key, request)
 * when a request relevant to the given key occurs. This triggers bookeeping logic and returns back any requests satisfied by this
 * new request.
 *
 * An implementation provides extends two helper functions
 *   def checkSatisfied(request: R, delayed: T): Boolean
 * this function returns true if the given request (in combination with whatever previous requests have happened) satisfies the delayed
 * request delayed. This method will likely also need to do whatever bookkeeping is necessary.
 *
 * The second function is
 *   def expire(delayed: T)
 * this function handles delayed requests that have hit their time limit without being satisfied.
 *
 */
public abstract class RequestPurgatory<T extends  DelayedRequest, R> {

    private static Logger logger = Logger.getLogger(RequestPurgatory.class);

    public int brokerId = 0;
    public int purgeInterval = 10000;

    public RequestPurgatory(int brokerId, int purgeInterval) {
        this.brokerId = brokerId;
        this.purgeInterval = purgeInterval;
        expirationThread = Utils.newThread("request-expiration-task",expiredRequestReaper, false);
        expirationThread.start();
    }

    /* a list of requests watching each key */
    private Pool<Object, Watchers> watchersForKey = new Pool<>();

    private AtomicInteger requestCounter = new AtomicInteger(0);

    /* background thread expiring requests that have been waiting too long */
    private ExpiredRequestReaper expiredRequestReaper = new ExpiredRequestReaper();

    private Thread expirationThread ;


    /**
     * Add a new delayed request watching the contained keys
     */
    public void watch(T delayedRequest) {
        requestCounter.getAndIncrement();

        for(Object key : delayedRequest.keys) {
            Watchers lst = watchersFor(key);
            lst.add(delayedRequest);
        }
        expiredRequestReaper.enqueue(delayedRequest);
    }

    /**
     * Update any watchers and return a list of newly satisfied requests.
     */
    public List<T> update(Object key, R request) throws Exception {
        Watchers w = watchersForKey.get(key);
        if(w == null)
            return new ArrayList<>();
        else
            return w.collectSatisfiedRequests(request);
    }

    private Watchers watchersFor(Object key){
        Watchers watchers = watchersForKey.putIfNotExists(key,new Watchers());
        if(watchers == null){
            return watchersForKey.get(key);
        }
        return watchers;
    }

    /**
     * Check if this request satisfied this delayed request
     */
    protected abstract boolean checkSatisfied(R request, T delayed) throws Exception;

    /**
     * Handle an expired delayed request
     */
    protected abstract void expire(T delayed) throws Exception;

    /**
     * Shutdown the expirey thread
     */
    public void shutdown() throws InterruptedException {
        expiredRequestReaper.shutdown();
    }

    /**
     * A linked list of DelayedRequests watching some key with some associated
     * bookkeeping logic.
     */
    private class Watchers {


        private List<T> requests = new ArrayList<>();

        public int numRequests(){
            return requests.size();
        }

        public synchronized void add(T t) {
            requests.add(t);
        }

        public synchronized int purgeSatisfied() {
            Iterator<T> iter = requests.iterator();
            int purged = 0;
            while(iter.hasNext()) {
                T curr = iter.next();
                if(curr.satisfied.get()) {
                    iter.remove();
                    purged += 1;
                }
            }
            return purged;
        }

        public synchronized List<T> collectSatisfiedRequests(R request) throws Exception {
            List<T> response = new ArrayList<>();
            Iterator<T> iter = requests.iterator();
            while(iter.hasNext()) {
                T curr = iter.next();
                if(curr.satisfied.get()) {
                    // another thread has satisfied this request, remove it
                    iter.remove();
                } else {
                    // synchronize on curr to avoid any race condition with expire
                    // on client-side.
                    if(checkSatisfied(request, curr)) {
                        iter.remove();
                        boolean updated = curr.satisfied.compareAndSet(false, true);
                        if(updated ) {
                            response.add(curr);
                            expiredRequestReaper.satisfyRequest();
                        }
                    }
                }
            }
            return response;
        }
    }


    /**
     * Runnable to expire requests that have sat unfullfilled past their deadline
     */
    private class ExpiredRequestReaper implements Runnable  {

        private DelayQueue<T> delayed = new DelayQueue<T>();
        private AtomicBoolean running = new AtomicBoolean(true);
        private CountDownLatch shutdownLatch = new CountDownLatch(1);

        /* The count of elements in the delay queue that are unsatisfied */
        private AtomicInteger unsatisfied = new AtomicInteger(0);

        public int numRequests(){
            return delayed.size();
        }

        /** Main loop for the expiry thread */
        public void run() {
            while(running.get()) {
                try {
                    T curr = pollExpired();
                    if (curr != null) {
                        synchronized(curr) {
                            expire(curr);
                        }
                    }
                    if (requestCounter.get() >= purgeInterval) { // see if we need to force a full purge
                        requestCounter.set(0);
                        int purged = purgeSatisfied();
                        logger.debug(String.format("Purged %d requests from delay queue.",purged));
                        List<Integer> list = watchersForKey.values().stream().map(x->purgeSatisfied()).collect(Collectors.toList());
                        int numPurgedFromWatchers = list.stream().mapToInt(x->x).sum();
                        logger.debug(String.format("Purged %d (watcher) requests.",numPurgedFromWatchers));
                    }
                } catch (Exception e){
                    logger.error("Error in long poll expiry thread: ", e);
                }
            }
            shutdownLatch.countDown();
        }

        /** Add a request to be expired */
        public void enqueue(T t) {
            delayed.add(t);
            unsatisfied.incrementAndGet();
        }

        /** Shutdown the expiry thread*/
        public void shutdown() throws InterruptedException {
            logger.debug("Shutting down.");
            running.set(false);
            shutdownLatch.await();
            logger. debug("Shut down complete.");
        }

        /** Record the fact that we satisfied a request in the stats for the expiry queue */
        public void satisfyRequest(){
            unsatisfied.getAndDecrement();
        }

        /**
         * Get the next expired event
         */
        private T pollExpired() throws InterruptedException {
            while(true) {
                T curr = delayed.poll(200L, TimeUnit.MILLISECONDS);
                if (curr == null)
                    return null;
                boolean updated = curr.satisfied.compareAndSet(false, true);
                if(updated) {
                    unsatisfied.getAndDecrement();
                    return curr;
                }
            }
        }

        /**
         * Delete all expired events from the delay queue
         */
        private int purgeSatisfied() {
            int purged = 0;
            Iterator<T> iter = delayed.iterator();
            while(iter.hasNext()) {
                T curr = iter.next();
                if(curr.satisfied.get()) {
                    iter.remove();
                    purged += 1;
                }
            }
            return purged;
        }
    }
}

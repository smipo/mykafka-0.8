package kafka.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IteratorTemplate<T> implements Iterator<T>  {

    private State state = State.NOT_READY;

    private T nextItem;

    public T next() {
        if(!hasNext())
            throw new NoSuchElementException();
        state = State.NOT_READY;
        if(nextItem == null){
            throw new IllegalStateException("Expected item but none found.");
        }
        return nextItem;
    }

    public boolean hasNext(){
        if(state == State.FAILED)
            throw new IllegalStateException("Iterator is in failed state");
        if(state == State.DONE ){
            return false;
        }else if(state == State.READY){
            return true;
        }
        return  maybeComputeNext();
    }

    protected abstract T makeNext() ;

    private boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if(state == State.DONE) {
            return false;
        }
        state = State.READY;
        return true;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }

    protected void resetState() {
        state = State.NOT_READY;
    }
    enum State{
        DONE,
        READY,
        NOT_READY,
        FAILED;
    }
}

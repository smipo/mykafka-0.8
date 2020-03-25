package kafka.network;


public abstract class Transmission {

    /**
     * Is this send complete?
     */
    public abstract boolean complete();

    protected void expectIncomplete(){
        if(complete())
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
    }

    protected void expectComplete() {
        if(!complete())
            throw new IllegalStateException("This operation cannot be completed on an incomplete request.");
    }
}

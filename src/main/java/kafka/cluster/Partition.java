package kafka.cluster;

public class Partition {

    public static Partition parse(String s) {
        String[] pieces = s.split("-");
        if(pieces.length != 2)
            throw new IllegalArgumentException("Expected name in the form x-y.");
        return new Partition(Integer.parseInt(pieces[0]), Integer.parseInt(pieces[1]));
    }

    protected int brokerId;
    protected int partId;

    public Partition(int brokerId,int partId) {
        this.brokerId = brokerId;
        this.partId = partId;
    }

    public int brokerId() {
        return brokerId;
    }

    public int partId() {
        return partId;
    }

    public String name() {
        return brokerId + "-" + partId;
    }

    @Override
    public String toString(){
        return name();
    }

    public int compare(Partition that) {
        if (this.brokerId == that.brokerId)
            return this.partId - that.partId;
        else
            return this.brokerId - that.brokerId;
    }


    @Override
    public boolean equals(Object obj){
        if(obj == null ) return false;
        if(obj instanceof Partition){
            Partition that = (Partition)obj;
            return brokerId == that.brokerId && partId == that.partId;
        }
        return false;
    }


    @Override
    public int hashCode(){
        return 31 * (17 + brokerId) + partId;
    }
}

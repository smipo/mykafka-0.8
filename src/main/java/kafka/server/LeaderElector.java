package kafka.server;
/**
 * This trait defines a leader elector If the existing leader is dead, this class will handle automatic
 * re-election and if it succeeds, it invokes the leader state change callback
 */
public interface LeaderElector {

    public void startup();

    public boolean amILeader();

    public boolean elect();

    public void  close();
}

package kafka.common;

public class LeaderElectionNotNeededException extends RuntimeException {
    public LeaderElectionNotNeededException() {
        super();
    }


    public LeaderElectionNotNeededException(String message) {
        super(message);
    }
}

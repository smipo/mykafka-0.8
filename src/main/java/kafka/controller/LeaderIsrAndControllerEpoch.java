package kafka.controller;

import kafka.api.LeaderAndIsrRequest;

public class LeaderIsrAndControllerEpoch {

    public LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr;
    public int controllerEpoch;

    public LeaderIsrAndControllerEpoch(LeaderAndIsrRequest.LeaderAndIsr leaderAndIsr, int controllerEpoch) {
        this.leaderAndIsr = leaderAndIsr;
        this.controllerEpoch = controllerEpoch;
    }
}

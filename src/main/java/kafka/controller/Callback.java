package kafka.controller;

import kafka.api.RequestOrResponse;

public interface Callback<T extends RequestOrResponse> {

    void onCallback(T t);
}

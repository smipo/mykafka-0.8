package kafka.controller;

public interface Callback<T> {

    void onCallback(T t);
}

package kafka.utils;

public class Three<F,S,T> {

    private F f;
    private S s;
    private T t;

    public Three(F f, S s, T t) {
        this.f = f;
        this.s = s;
        this.t = t;
    }

    public F F() {
        return f;
    }

    public S S() {
        return s;
    }

    public T T() {
        return t;
    }
}

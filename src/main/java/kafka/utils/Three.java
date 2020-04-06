package kafka.utils;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Three<?, ?, ?> three = (Three<?, ?, ?>) o;
        return Objects.equals(f, three.f) &&
                Objects.equals(s, three.s) &&
                Objects.equals(t, three.t);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f, s, t);
    }
}

package kafka.utils;

import java.util.ArrayList;
import java.util.Collections;

import static kafka.utils.Preconditions.*;

public class Lists {

    /**
     * Creates a mutable, empty {@code ArrayList} instance.
     */
    public static <E> ArrayList<E> newArrayList() {
        return new ArrayList<>();
    }

    /**
     * Creates a mutable {@code ArrayList} instance containing the given elements.
     */
    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> newArrayList(E... elements) {
        checkNotNull(elements);
        // Avoid integer overflow when a large array is passed in
        ArrayList<E> list = new ArrayList<>();
        Collections.addAll(list, elements);
        return list;
    }
}

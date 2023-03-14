package net.sentientturtle.csv;

import java.util.function.Function;

/**
 * Alternative to {@link Function} to permit checked exceptions. Equivalent usage.
 * <br>
 * See {@link Function} documentation
 */
@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;

    /**
     * Equivalent to {@link Function#identity()}
     */
    static <T> ThrowingFunction<T, ? super T> identity() {
        return t -> t;
    }
}

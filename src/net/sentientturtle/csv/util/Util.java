package net.sentientturtle.csv.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Contains various utility functions
 */
public class Util {
    /**
     * Asserts array and it's values are non-null
     */
    public static <T> T[] requireNonNullValues(@NotNull T @NotNull [] array) {
        return requireNonNullValues(array, false);
    }

    /**
     * Asserts array values are non-null
     * @param arrayNullable If true, array may be null
     */
    public static <T> T[] requireNonNullValues(@NotNull T @Nullable [] array, boolean arrayNullable) {
        if (arrayNullable && array == null) {
            return array;
        } else if (!arrayNullable && array == null) {
            throw new NullPointerException("array must be non-null");
        } else {
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) throw new NullPointerException("array contents must be non-null; null found at index " + i);
            }
            return array;
        }
    }

    /**
     * Asserts map keys and values are not null
     */
    public static <K, V> Map<K, V> requireNonNullEntries(@NotNull Map<K, V> map) {
        Objects.requireNonNull(map);
        if (map.containsKey(null)) throw new NullPointerException("Map keys must be non-null"); // Check key first as key access does not require iterating the map
        if (map.containsValue(null)) throw new NullPointerException("Map values must be non-null");
        return map;
    }

    /**
     * Throws checked exception as if it were a runtime exception
     */
    @SuppressWarnings("unchecked")
    public static <T, E extends Throwable> T sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}

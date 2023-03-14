package net.sentientturtle.reflectiontest;

import net.sentientturtle.csv.reflection.TypeToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Test of {@link TypeToken}
 * <br>
 * {@link TypeToken#TypeToken()} is intended to be only used through (anonymous) subclass, even within the same package.
 * As such, this test is located in a different package to make such erroneous usage a compiler error, rather than merely a runtime exception.
 */
public class TypeTokenTest {
    @Test
    public void tokenCreation() {
        new TypeToken<>(Object.class);
        new TypeToken<Object>() {};
        new TypeToken<>(int.class);
        new TypeToken<>(Object[].class);
        new TypeToken<>(int[].class);
        new TypeToken<List<Integer>>() {};
        new TypeToken<List<? extends Number>>() {};

        // No subclassing when using the class constructor
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<>(Object.class) {});
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<>(List.class));
        //noinspection rawtypes
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<List>() {});
    }

    @Test
    public <T> void tokenCreationInGenericMethod() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<T>() {});
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<T[]>() {});
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<List<T>>() {});
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeToken<List<? extends T>>() {});
    }

    @Test
    public void equality() {
        // Raw type equality
        TypeToken<Object> token = new TypeToken<>(Object.class);
        TypeToken<Object> otherToken = new TypeToken<>() {};
        TypeToken<Object[]> arrayToken = new TypeToken<>(Object[].class);
        TypeToken<Object[]> otherArrayToken = new TypeToken<>() {};
        TypeToken<int[]> primitiveArrayToken = new TypeToken<>(int[].class);
        TypeToken<String[]> differentArrayToken = new TypeToken<>(String[].class);

        Assertions.assertEquals(token, token);                  // this == this
        Assertions.assertEquals(token, otherToken);             // Object == Object, also check subclass constructor matches class constructor
        Assertions.assertEquals(arrayToken, otherArrayToken);   // Object[] == Object[]
        Assertions.assertNotEquals(token, arrayToken);                  // Object != Object[]
        Assertions.assertNotEquals(arrayToken, primitiveArrayToken);    // Object[] != int[]
        Assertions.assertNotEquals(arrayToken, differentArrayToken);    // Object[] != String[]

        // Parameterized type equality
        TypeToken<List<Object>> listToken = new TypeToken<>() {};
        TypeToken<List<Object>> otherListToken = new TypeToken<>() {};
        TypeToken<List<String>> differentListToken = new TypeToken<>() {};

        Assertions.assertEquals(listToken, otherListToken);         // List<Object> == List<Object>
        Assertions.assertNotEquals(listToken, differentListToken);  // List<Object> != List<String>

        // Parameterized array type equality
        TypeToken<List<String>[]> listArrayToken = new TypeToken<>() {};
        TypeToken<List<String>[]> otherListArrayToken = new TypeToken<>() {};

        Assertions.assertEquals(listArrayToken, otherListArrayToken);   // List<String>[] == List<String>[]
        Assertions.assertNotEquals(listArrayToken, arrayToken);         // List<String>[] != Object[]

        // Generic bound equality
        TypeToken<List<? extends Number>> boundedToken = new TypeToken<>() {};
        TypeToken<List<? extends Number>> otherBoundedToken = new TypeToken<>() {};
        TypeToken<List<?>> broaderBoundedToken = new TypeToken<>() {};
        TypeToken<List<? extends Integer>> narrowerBoundedToken = new TypeToken<>() {};

        TypeToken<List<? super Number>> lowerBoundedToken = new TypeToken<>() {};
        TypeToken<List<? super Integer>> narrowerLowerBoundedToken = new TypeToken<>() {};

        Assertions.assertEquals(boundedToken, otherBoundedToken);                   // List<? extends Number> == List<? extends Number>
        Assertions.assertNotEquals(boundedToken, narrowerBoundedToken);             // List<? extends number> != List<?>
        Assertions.assertNotEquals(listToken, broaderBoundedToken);                 // List<?> != List<Object>
        Assertions.assertNotEquals(boundedToken, broaderBoundedToken);              // List<? extends Number> != List<? extends Integer>
        Assertions.assertNotEquals(boundedToken, lowerBoundedToken);                // List<? extends Number> != List<? super Integer>
        Assertions.assertNotEquals(lowerBoundedToken, narrowerLowerBoundedToken);   // List<? super Number> != List<? super Integer>
    }

    @Test
    public void tokenType() {
        TypeToken<List<?>> token = new TypeToken<>() {};
        Assertions.assertTrue(token.isConcreteType());
        Assertions.assertFalse(token.isGenericBound());
        Assertions.assertFalse(token.isRecord());

        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> genericBound = token.getGenericParameters()[0];
        Assertions.assertTrue(genericBound.isGenericBound());
        Assertions.assertFalse(genericBound.isConcreteType());
        Assertions.assertFalse(genericBound.isRecord());
    }

    @Test
    public void rawType() {
        TypeToken<Object> token = new TypeToken<>(Object.class);
        TypeToken<Object> otherToken = new TypeToken<>() {};
        TypeToken<List<Object>> listToken = new TypeToken<>() {};

        Assertions.assertEquals(token.getRawType(), Object.class);
        Assertions.assertEquals(otherToken.getRawType(), Object.class);
        Assertions.assertEquals(listToken.getRawType(), List.class);
    }

    @Test
    public void genericParameters() {
        TypeToken<Object> token = new TypeToken<>(Object.class);
        TypeToken<List<String>> listToken = new TypeToken<>() {};
        TypeToken<Map<Integer, String>> mapToken = new TypeToken<>() {};

        TypeToken<List<?>> boundListToken = new TypeToken<>() {};
        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> genericBound = boundListToken.getGenericParameters()[0];

        Assertions.assertArrayEquals(token.getGenericParameters(), new TypeToken[]{});
        Assertions.assertArrayEquals(listToken.getGenericParameters(), new TypeToken[]{new TypeToken<>(String.class)});
        Assertions.assertArrayEquals(mapToken.getGenericParameters(), new TypeToken[]{new TypeToken<>(Integer.class), new TypeToken<>(String.class)});

        Assertions.assertNull(genericBound.getGenericParameters());
    }

    @Test
    public void superType() {
        TypeToken<ArrayList<Integer>> list = new TypeToken<>() {};
        TypeToken<AbstractList<Integer>> abstractList = new TypeToken<>() {};
        TypeToken<AbstractCollection<Integer>> abstractCollection = new TypeToken<>() {};
        TypeToken<Object> object = new TypeToken<>(Object.class);
        Assertions.assertEquals(list.getSupertype(), abstractList);
        Assertions.assertEquals(abstractList.getSupertype(), abstractCollection);
        Assertions.assertEquals(abstractCollection.getSupertype(), object);
        Assertions.assertNull(object.getSupertype());
    }

    private record TestRecord(int anInt, String aString) {}

    private record GenericRecord<S>(int anInt, S aString) {}

    @Test
    public void records() {
        TypeToken<TestRecord> record = new TypeToken<>() {};
        Assertions.assertTrue(record.isRecord());

        TypeToken.RecordField[] recordComponents = record.getRecordComponents();
        Assertions.assertNotNull(recordComponents);
        Assertions.assertArrayEquals(recordComponents, new TypeToken.RecordField[]{
                new TypeToken.RecordField("anInt", new TypeToken<>(int.class)),
                new TypeToken.RecordField("aString", new TypeToken<String>() {})
        });

        TypeToken<GenericRecord<String>> genericRecord = new TypeToken<>() {};
        Assertions.assertTrue(genericRecord.isRecord());

        recordComponents = genericRecord.getRecordComponents();
        Assertions.assertNotNull(recordComponents);
        Assertions.assertArrayEquals(recordComponents, new TypeToken.RecordField[]{
                new TypeToken.RecordField("anInt", new TypeToken<>(int.class)),
                new TypeToken.RecordField("aString", new TypeToken<String>() {})
        });


        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> genericBound = new TypeToken<List<?>>() {}.getGenericParameters()[0];
        Assertions.assertThrows(IllegalStateException.class, genericBound::getRecordComponents);
    }

    @Test
    public void stringRepresentation() {
        TypeToken<Integer> primitiveToken = new TypeToken<>(int.class);
        Assertions.assertEquals(primitiveToken.toString(), int.class.getName());

        TypeToken<Object> objectToken = new TypeToken<>(Object.class);
        Assertions.assertEquals(objectToken.toString(), Object.class.getName());

        TypeToken<Object[]> arrayToken = new TypeToken<>(Object[].class);
        Assertions.assertEquals(arrayToken.toString(), Object.class.getName() + "[]");

        TypeToken<Object[][][][]> multiArrayToken = new TypeToken<>(Object[][][][].class);
        Assertions.assertEquals(multiArrayToken.toString(), Object.class.getName() + "[][][][]");

        TypeToken<List<String>> parameterizedToken = new TypeToken<>() {};
        Assertions.assertEquals(parameterizedToken.toString(), List.class.getName() + "<" + String.class.getName() + ">");

        TypeToken<Map<Integer, String>> twoParameterizedToken = new TypeToken<>() {};
        Assertions.assertEquals(twoParameterizedToken.toString(), Map.class.getName() + "<" + Integer.class.getName() + ", " + String.class.getName() + ">");

        TypeToken<List<String>[]> parameterizedArrayToken = new TypeToken<>() {};
        Assertions.assertEquals(parameterizedArrayToken.toString(), List.class.getName() + "<" + String.class.getName() + ">[]");

        TypeToken<List<?>> boundedToken = new TypeToken<>() {};
        Assertions.assertEquals(boundedToken.toString(), List.class.getName() + "<?>");

        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> genericBound = boundedToken.getGenericParameters()[0];
        Assertions.assertEquals(genericBound.toString(), "?");
        Assertions.assertEquals(genericBound.asArray().toString(), "?[]");


        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> lowerBound = new TypeToken<List<? super Integer>>() {}.getGenericParameters()[0];
        Assertions.assertEquals(lowerBound.toString(), "? super " + Integer.class.getName());
        Assertions.assertEquals(lowerBound.asArray().toString(), "?[] super " + Integer.class.getName());

        @SuppressWarnings("DataFlowIssue")
        TypeToken<?> upperBound = new TypeToken<List<? extends Number>>() {}.getGenericParameters()[0];
        Assertions.assertEquals(upperBound.toString(), "? extends " + Number.class.getName());
        Assertions.assertEquals(upperBound.asArray().toString(), "?[] extends " + Number.class.getName());
    }
}

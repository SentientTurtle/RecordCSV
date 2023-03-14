package net.sentientturtle.csv;

import net.sentientturtle.csv.exception.CharParseException;
import net.sentientturtle.csv.reflection.TypeToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link CSVMapper}
 * <br>
 * CAUTION: These tests depending on CSVReader, if tests both here and in {@link CSVReaderTest} fail, the latter should be resolved first
 */
public class CSVMapperTest {
    public record TestRecord(String one, int two, double three) {}

    public record StringRecord(String one, String two, String three) {}

    public record GenericRecord<T, U, V>(T one, U two, V three) {}

    private CSVReader.Builder createTestReader() {
        return new CSVReader.Builder()
                       .setUnitSeparator(',')
                       .setRecordSeparator('\n')
                       .trimWhitespace(false)
                       .setWhitespaceDefinition(Character::isWhitespace)
                       .setQuoteMode(CSVReader.QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
    }

    private <R extends Record> CSVMapper.Builder<R> createTestMapper(TypeToken<R> type, boolean hasHeader) {
        return createTestReader()
                       .mapped(type, hasHeader)
                       .setHeaderCompareFunction(String::equalsIgnoreCase)
                       .inferEmptyTrailingColumns(false)
                       .ignoreExcessColumns(false);
    }

    private record PrivateRecord() {}

    @Test
    public void construction() {
        var testMapper = createTestMapper(new TypeToken<>(TestRecord.class), false);
        var genericTestMapper = createTestMapper(new TypeToken<GenericRecord<String, Integer, Double>>() {}, false);
        Assertions.assertThrows(IllegalArgumentException.class, () -> createTestMapper(new TypeToken<>(Record.class), false));

        // Cannot access private record constructor
        Assertions.assertThrows(IllegalStateException.class, () -> createTestMapper(new TypeToken<>(PrivateRecord.class), false).validate());
    }

    private static final String SINGLE_LINE = "value1,1,2.0";

    @Test
    public void readSingleLine() throws IOException {
        CSVMapper<?> testMapper = createTestMapper(new TypeToken<>(TestRecord.class), false).build(SINGLE_LINE);
        Assertions.assertIterableEquals(testMapper, List.of(new TestRecord("value1", 1, 2.0)));

        CSVMapper<?> stringMapper = createTestMapper(new TypeToken<>(StringRecord.class), false).build(SINGLE_LINE);
        Assertions.assertIterableEquals(stringMapper, List.of(new StringRecord("value1", "1", "2.0")));

        CSVMapper<?> genericMapper = createTestMapper(new TypeToken<GenericRecord<String, Integer, Double>>() {}, false)
                                             .build(SINGLE_LINE);
        Assertions.assertIterableEquals(genericMapper, List.of(new GenericRecord<String, Integer, Double>("value1", 1, 2.0)));
    }


    private static final String HEADER_AND_LINE = "one,two,three\nvalue1,1,2.0";
    private static final String MIXED_HEADER_AND_LINE = "one,three,two\nvalue1,2.0,1";
    private static final String MISSING_HEADER = "value1,2.0,1\nvalue1,2.0,1";

    @Test
    public void readHeader() throws IOException {
        CSVMapper.Builder<?> testMapper = createTestMapper(new TypeToken<>(TestRecord.class), true);
        Assertions.assertIterableEquals(
                testMapper.build(HEADER_AND_LINE),
                List.of(new TestRecord("value1", 1, 2.0))
        );
        Assertions.assertIterableEquals(
                testMapper.build(MIXED_HEADER_AND_LINE),
                List.of(new TestRecord("value1", 1, 2.0))
        );
        Assertions.assertThrows(CSVParseException.class, () -> testMapper.build(MISSING_HEADER).readRecord());
    }

    public record PrimitiveTypes(
            byte bytePrimitive,
            short shortPrimitive,
            int intPrimitive,
            long longPrimitive,
            float floatPrimitive,
            double doublePrimitive,
            boolean booleanPrimitive,
            char charPrimitive
    ) {}

    public record BoxedTypes(
            Byte byteBoxed,
            Short shortBoxed,
            Integer intBoxed,
            Long longBoxed,
            Float floatBoxed,
            Double doubleBoxed,
            Boolean booleanBoxed,
            Character character
    ) {}

    public record CharRecord(char character) {}

    private static final String DEFAULT_TYPE_VALUES = "1,2,3,4,5.0,6.0,true,a";
    private static final String CHAR_MAX = String.valueOf(Character.MAX_VALUE);
    private static final String CHAR_OVER_MAX = "\uD83E\uDD14";

    @Test
    public void defaultFieldTypeMappers() throws Exception {
        Assertions.assertIterableEquals(
                createTestMapper(new TypeToken<>(PrimitiveTypes.class), false).build(DEFAULT_TYPE_VALUES),
                List.of(new PrimitiveTypes((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, 'a'))
        );
        Assertions.assertIterableEquals(
                createTestMapper(new TypeToken<>(BoxedTypes.class), false).build(DEFAULT_TYPE_VALUES),
                List.of(new BoxedTypes((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true, 'a'))
        );
        Assertions.assertIterableEquals(
                createTestMapper(new TypeToken<>(CharRecord.class), false).build(CHAR_MAX),
                List.of(new CharRecord(Character.MAX_VALUE))
        );
        Assertions.assertThrows(CharParseException.class, () -> createTestMapper(new TypeToken<>(CharRecord.class), false).build(CHAR_OVER_MAX).readRecord());
    }


    private static final String USER_TYPE_VALUES = "1,2,3";

    private static class UserType {
        @Override
        public boolean equals(Object obj) {
            return obj instanceof UserType;
        }
    }

    @Test
    public void userFieldTypeMappers() throws Exception {
        Assertions.assertThrows(IllegalStateException.class, () -> createTestMapper(new TypeToken<GenericRecord<UserType, Void, Void>>() {}, false).validate());

        CSVMapper<?> mapper = createTestMapper(new TypeToken<GenericRecord<UserType, UserType, UserType>>() {}, false)
                                      .addTypeMapper(new TypeToken<>(UserType.class), string -> new UserType())
                                      .build(USER_TYPE_VALUES);

        Assertions.assertIterableEquals(List.of(new GenericRecord<>(new UserType(), new UserType(), new UserType())), mapper);
    }

    private static final String THREE_VALUE_HEADER_AND_LINE = "one,two,three\nvalue1,value2,value3";
    private static final String FOUR_VALUE_HEADER_AND_THREE_VALUE_LINE = "one,two,three,four\nvalue1,value2,value3";
    private static final String FOUR_VALUE_HEADER_AND_LINE = "one,two,three,four\nvalue1,value2,value3,value4";
    private static final String FIVE_VALUE_HEADER_AND_LINE = "one,two,three,four,five\nvalue1,value2,value3,value4,value5";

    public record FourStringRecord(String one, String two, String three, String four) {}

    @Test
    public void mismatchedColumnCount() throws Exception {
        // Expect header, and permit only exact column count match
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(false)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(false)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(false)
                              .build(FIVE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        // Expect header, and permit equal or greater column count match
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .ignoreExcessColumns(true)
                              .inferEmptyTrailingColumns(false)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FIVE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        // Expect header, and permit equal or lesser column count match
        Assertions.assertThrows(    // Still throw an error; We can't fill missing headers with null
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(true)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(true)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .readHeader(true)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(true)
                              .build(FIVE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        // Expect header, and permit any column count
        Assertions.assertThrows(    // Still throw an error; We can't fill missing headers with null
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                              .ignoreExcessColumns(true)
                              .inferEmptyTrailingColumns(true)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(true)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("value1", "value2", "value3", "value4"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), true)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FIVE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );


        // No-header

        // No header and permit only exact column count match
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(false)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(false)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(false)
                              .build(FIVE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        // No header and permit equal or greater column count match
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                              .ignoreExcessColumns(true)
                              .inferEmptyTrailingColumns(false)
                              .build(THREE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FIVE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        // No header and permit equal or lesser column count match
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", ""),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(true)
                        .build(THREE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(true)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertThrows(
                CSVParseException.class,
                () -> createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                              .ignoreExcessColumns(false)
                              .inferEmptyTrailingColumns(true)
                              .build(FIVE_VALUE_HEADER_AND_LINE)
                              .readRecord()
        );
        // No header and permit any column count
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", ""),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(false)
                        .inferEmptyTrailingColumns(true)
                        .build(THREE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(true)
                        .build(FOUR_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
        Assertions.assertEquals(
                new FourStringRecord("one", "two", "three", "four"),
                createTestMapper(new TypeToken<>(FourStringRecord.class), false)
                        .ignoreExcessColumns(true)
                        .inferEmptyTrailingColumns(false)
                        .build(FIVE_VALUE_HEADER_AND_LINE)
                        .readRecord()
        );
    }
}

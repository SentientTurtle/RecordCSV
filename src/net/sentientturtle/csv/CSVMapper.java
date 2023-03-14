package net.sentientturtle.csv;

import net.sentientturtle.csv.exception.BooleanParseException;
import net.sentientturtle.csv.exception.CharParseException;
import net.sentientturtle.csv.reflection.TypeToken;
import net.sentientturtle.csv.util.Util;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * CSVMapper is the concrete type for a "mapped" CSVReader, parsing lines of character-separated-values documents to records
 * <br>
 * Created with {@link CSVMapper.Builder}, in turn created by {@link CSVReader.Builder#mapped(TypeToken, boolean)}
 * <br>
 * Implements {@link Iterable} and can be used in for-each loops.
 * <br>
 * CAUTION: CSVMapper can only be iterated once. Each row of the document will be yielded once, even when multiple iterators from {@link CSVMapper#iterator()} are created. To iterate multiple times, collect each record to a list.
 * <br><br>
 * Example usage:
 * <pre>
 * record ARecord(int aValue, int otherValue) {}
 * try (
 *     CSVMapper&lt;ARecord&gt; csv = CSVReader.Builder.COMMA_SEPARATED()
 *         .mapped(ARecord.class, true)
 *          .build(new FileReader("./file.csv"))
 * ) {
 *   for (ARecord record : csv) {
 *     // Use record
 *   }
 * } catch (Exception error) {
 *     // Handle IO, parsing, or type mapping error
 * }
 * </pre>
 *
 * @param <R> Type of record provided by this mapper
 */
public class CSVMapper<R extends Record> implements Iterable<R>, AutoCloseable {
    // Input
    private final CSVReader reader;
    private final Iterator<String[]> csvIter;
    // Configuration
    private final boolean ignoreExcessColumns;
    private final boolean inferEmptyTrailingColumns;
    private final @NotNull String @Nullable [] headerFields;
    private final BiFunction<String, String, Boolean> headerCompareFunction;
    private final ThrowingFunction<String, Object>[] fieldMappers;
    private final ThrowingFunction<Object[], R> recordMapper;
    // State
    private boolean mustReadHeader;
    // Column indices for the record fields; int #N in this array specifies which column is used for field N in the record.
    // If null, no re-ordering of columns is done, and the first N units are passed into the record constructor
    private int @Nullable [] fieldColumnIndices;

    /**
     * Private constructor; This type is initialized through {@link CSVMapper.Builder}
     *
     * @param reader                    Backing {@link CSVReader} used to parse CSV data
     * @param readHeader                If true, parse the first row of the CSV document as a header
     * @param ignoreExcessColumns       If true, ignore excess columns. If expecting a header, ignores all columns not specifies in `csvColumns`, else, ignores all columns after fieldMappers.length
     * @param inferEmptyTrailingColumns If true, insert empty-string as value for missing columns. If expecting a header, all header fields must still be present, only data fields may be missing
     * @param headerCompareFunction     Function used to compare headers, usually either {@link String#equals(Object) String::equals} or {@link String#equalsIgnoreCase(String) String::equalsIgnoreCase}
     * @param headerFields              Header fields that are expected, may be null even if `readHeader` is true. If `readHeader` is true and `headerFields` is null, the first line of the csv document is simply discarded
     * @param fieldMappers              Type mappers for fields. If `headerFields` is set, order must match that of `headerFields`
     * @param recordMapper              Record constructor, takes array created by `fieldMappers`
     */
    private CSVMapper(
            CSVReader reader,
            boolean readHeader,
            boolean ignoreExcessColumns,
            boolean inferEmptyTrailingColumns,
            BiFunction<String, String, Boolean> headerCompareFunction,
            @NotNull String @Nullable [] headerFields,
            ThrowingFunction<String, Object>[] fieldMappers,
            ThrowingFunction<Object[], R> recordMapper
    ) {
        this.reader = Objects.requireNonNull(reader);
        this.csvIter = reader.iterator();
        this.ignoreExcessColumns = ignoreExcessColumns;
        this.inferEmptyTrailingColumns = inferEmptyTrailingColumns;
        this.headerFields = Util.requireNonNullValues(headerFields, true);
        this.headerCompareFunction = headerCompareFunction;
        this.fieldMappers = fieldMappers;
        this.recordMapper = recordMapper;
        this.mustReadHeader = readHeader;
    }

    /**
     * Read a header from the csv file.
     * <br>
     * {@link CSVMapper#headerFields} is null, a single line/row is read and "blindly" discarded with no attempt to check if it forms a valid record
     *
     * @throws IOException       if an underlying error in the backing CSVReader occurs
     * @throws CSVParseException Header is missing, or does not match expected columns
     */
    private synchronized void readHeader() throws IOException, CSVParseException {
        mustReadHeader = false;
        if (!csvIter.hasNext()) throw new CSVParseException("header expected, found end of stream");
        String[] header = reader.readLine();
        if (headerFields != null) {   // if csvColumns is null, we just throw away the header. This may result in the first row of a header-less CSV file being discarded if headers are expected.
            if (!ignoreExcessColumns && headerFields.length != header.length) {
                throw new CSVParseException("Expected " + headerFields.length + " columns, found " + header.length + "(`" + String.join(String.valueOf(reader.getUnitSeparator()), header) + "`)");
            }

            fieldColumnIndices = new int[headerFields.length];
            column_loop:
            for (int i = 0; i < headerFields.length; i++) {
                String column = headerFields[i];
                for (int j = 0; j < header.length; j++) {
                    if (headerCompareFunction.apply(header[j], column)) {
                        fieldColumnIndices[i] = j;
                        continue column_loop;
                    }
                }
                // Missing column
                throw new CSVParseException("could not find column (" + column + ") in CSV header (" + String.join(String.valueOf(reader.getUnitSeparator()), header) + ")");
            }
        }
    }

    /**
     * Read a single record, from a single row of the CSV document
     * <br>
     * Use of {@link CSVMapper#iterator()}, {@link CSVMapper#spliterator()}, or {@link CSVMapper#stream()} recommended for better handling of end-of-file
     *
     * @return Record representing the next
     * @throws NoSuchElementException If end-of-file has been reached
     * @throws IOException            If an error occurs while reading input
     * @throws Exception              If an error occurs while parsing a CSV row into Record {@link R}. Exception type depends on used fieldMappers
     */
    public synchronized R readRecord() throws NoSuchElementException, Exception {
        if (mustReadHeader) readHeader();   // TODO: Move to constructor so that iterators will not throw error

        String[] units = reader.readLine();
        Object[] fields;
        if (fieldColumnIndices != null) {
            fields = new Object[fieldColumnIndices.length];
            for (int i = 0; i < fieldColumnIndices.length; i++) {
                if (units.length > fieldColumnIndices[i]) {
                    fields[i] = fieldMappers[i].apply(units[fieldColumnIndices[i]]);
                } else if (inferEmptyTrailingColumns) {
                    fields[i] = fieldMappers[i].apply("");
                } else {
                    throw new CSVParseException("expected column #" + fieldColumnIndices[i] + " found only " + units.length + " @ row " + reader.rowCount());
                }
            }
        } else {
            fields = new Object[fieldMappers.length];
            if (units.length > fields.length && !ignoreExcessColumns) {
                throw new CSVParseException("expected " + fields.length + " columns, found " + units.length + " @ row " + reader.rowCount());
            } else if (units.length < fields.length && !inferEmptyTrailingColumns) {
                throw new CSVParseException("expected " + fields.length + " columns, found " + units.length + " @ row " + reader.rowCount());
            } else {
                for (int i = 0; i < Math.min(units.length, fields.length); i++) {
                    fields[i] = fieldMappers[i].apply(units[i]);
                }
                for (int i = units.length; i < fields.length; i++) {    // Infer missing fields
                    fields[i] = fieldMappers[i].apply("");
                }
            }
        }

        return recordMapper.apply(fields);
    }

    /**
     * @return True if this CSVMapper has not yet encountered end-of-file, and another record may be read
     */
    public boolean hasNext() {
        return csvIter.hasNext();
    }

    /**
     * Use this CSVMapper as an Iterator
     * <br><br>
     * CAUTION: CSVMapper can only be iterated once. Creating a new iterator does not reset iteration state, iterator will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" iterator, which may throw exceptions of any type depending on the Field Type Mappers used.
     *
     * @return Iterator equivalent of this CSVMapper.
     */
    @NotNull
    @Override
    public Iterator<R> iterator() {
        return new Iterator<R>() {
            /**
             * @return Delegates to {@link CSVMapper#hasNext()}
             */
            @Override
            public boolean hasNext() {
                return CSVMapper.this.hasNext();
            }

            /**
             * @return Delegates to {@link CSVMapper#readRecord()}
             * @throws Exception Any (even checked) exceptions may be thrown if a parsing error occurs.
             */
            @Override
            public R next() {
                try {
                    return CSVMapper.this.readRecord();
                } catch (Exception e) {
                    return Util.sneakyThrow(e);
                }
            }
        };
    }

    /**
     * Use this CSVMapper as a Spliterator
     * <br><br>
     * CAUTION: CSVMapper can only be iterated once. Creating a new spliterator does not reset iteration state, spliterator will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" spliterator, which may throw exceptions of any type depending on the Field Type Mappers used.
     *
     * @return Spliterator equivalent of this CSVMapper, having characteristics {@link Spliterator#ORDERED} and {@link Spliterator#NONNULL}
     */
    @Override
    public Spliterator<R> spliterator() {
        return Spliterators.spliteratorUnknownSize(
                this.iterator(),
                Spliterator.ORDERED | Spliterator.NONNULL
        );
    }

    /**
     * Use this CSVMapper as a Stream
     * <br><br>
     * CAUTION: CSVMapper can only be iterated once. Creating a new stream does not reset iteration state, stream will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" stream, which may throw exceptions of any type depending on the Field Type Mappers used.
     *
     * @param parallel if true, return a parallel stream
     * @return Stream equivalent of this CSVMapper.
     */
    public Stream<R> stream(boolean parallel) {
        return StreamSupport.stream(this.spliterator(), parallel);
    }

    /**
     * Closes this CSVMapper; Closing its input reader and preventing future reads
     * <br>
     * If this mapper is used as an iterator or (spliterator/stream), that iterator will no longer yield any records
     *
     * @throws IOException If an error occurs closing the backing reader
     */
    @Override
    public synchronized void close() throws Exception { // We do not throw Exception here, but it may be silently thrown by iteration. Inclusion here ensures a catch clause is present to handle exceptions during iteration
        this.reader.close();
    }

    /**
     * Builder for {@link CSVMapper}
     * <br>
     * Instantiated through the {@link CSVReader.Builder#mapped(TypeToken, boolean)} factory method, inheriting the CSV format and parsing behaviour from that Builder's CSVReader
     * <br>
     * See method documentation for details on configuration
     * <br>
     * Note: This builder may be used to create multiple readers. {@link Builder#validate()} may be used for fail-fast configuration validation
     *
     * @param <R> Type of record provided by this mapper
     */
    public static class Builder<R extends Record> {
        /**
         * Default field type mappers, provided for (boxed) primitive types and {@link String}
         * <ul>
         * <li> {@link String} mapper is performs no parsing; String field is passed as-is in the document, with whitespace trimmed as configured by {@link CSVReader.Builder#trimWhitespace(boolean)} </li>
         * <li> {@link Boolean} mapper accepts only `true` and `false`, any other string throws a parsing exception. Must be overridden if {@link Boolean#parseBoolean(String)} behaviour is desired instead </li>
         * <li> {@link Character} mapper accepts only strings with a single unicode codepoint <= {@link Character#MAX_VALUE}. Codepoints consisting of a surrogate pair, or strings with more than one codepoint will throw an exception </li>
         * <li> Numerical types use their respective #Parse[Type] methods, with radix 10. (e.g. {@link Byte#parseByte(String)}). Must be overridden if a different radix is required </li>
         * </ul>
         * NOTE: This is an unmodifiable map. Use {@link CSVMapper.Builder#addTypeMapper(TypeToken, ThrowingFunction)} to add type mappers
         * <br><br>
         * To override defaults, call {@link CSVMapper.Builder#addTypeMapper(TypeToken, ThrowingFunction)} with one of the default types.
         * <br>Example: (Hexadecimal integers)
         * <pre>builder.addTypeMapper(new TypeToken(int.class), string -> Integer.parseInt(string, 16))</pre>
         */
        public static final Map<TypeToken<?>, ThrowingFunction<String, Object>> DEFAULT_TYPE_MAPPERS;

        static {
            Map<TypeToken<?>, ThrowingFunction<String, Object>> map = new HashMap<>();
            map.put(new TypeToken<>(String.class), string -> string);
            map.put(new TypeToken<>(byte.class), Byte::parseByte);
            map.put(new TypeToken<>(Byte.class), Byte::parseByte);
            map.put(new TypeToken<>(short.class), Short::parseShort);
            map.put(new TypeToken<>(Short.class), Short::parseShort);
            map.put(new TypeToken<>(int.class), Integer::parseInt);
            map.put(new TypeToken<>(Integer.class), Integer::parseInt);
            map.put(new TypeToken<>(long.class), Long::parseLong);
            map.put(new TypeToken<>(Long.class), Long::parseLong);
            map.put(new TypeToken<>(float.class), Float::parseFloat);
            map.put(new TypeToken<>(Float.class), Float::parseFloat);
            map.put(new TypeToken<>(double.class), Double::parseDouble);
            map.put(new TypeToken<>(Double.class), Double::parseDouble);
            ThrowingFunction<String, Object> mapBoolean = string -> {
                if ("true".equalsIgnoreCase(string)) {
                    return true;
                } else if ("false".equalsIgnoreCase(string)) {
                    return false;
                } else {
                    throw new BooleanParseException("cannot read string `" + string + "` as boolean; Only 'true' and 'false' (in any capitalization) are accepted, if you need Boolean#parseBoolean behaviour, please override the field type mapper");
                }
            };
            map.put(new TypeToken<>(boolean.class), mapBoolean);
            map.put(new TypeToken<>(Boolean.class), mapBoolean);

            ThrowingFunction<String, Object> mapCharacter = string -> {
                if (string.codePointCount(0, Math.min(string.length(), 2)) > 1)
                    throw new CharParseException("cannot read string `" + string + "` with length " + string.codePointCount(0, string.length()) + "(codepoints) as single char"); // Including the string in the error is not ideal in a post-log4shell world, but parse[Type] functions do it as well, included for parity. TODO: Consider replacing all parseType functions
                int codepoint = string.codePointAt(0);
                if (codepoint > (int) Character.MAX_VALUE) throw new CharParseException(String.format("codepoint U+%X cannot fit into a single char; Please use a wrapper type if you are trying to read single codepoints", codepoint));
                return (char) codepoint;
            };
            map.put(new TypeToken<>(char.class), mapCharacter);
            map.put(new TypeToken<>(Character.class), mapCharacter);

            DEFAULT_TYPE_MAPPERS = Collections.unmodifiableMap(map);
        }

        private final CSVReader.Builder readerBuilder;
        private final TypeToken<R> recordType;

        private boolean readHeader;
        private boolean ignoreExcessColumns;
        private boolean inferEmptyTrailingColumns;
        private BiFunction<String, String, Boolean> headerCompareFunction;

        private final Map<TypeToken<?>, ThrowingFunction<String, Object>> typeMappers;

        private boolean isValidated;
        private String[] csvHeader;
        private ThrowingFunction<String, Object>[] fieldMappers;
        private ThrowingFunction<Object[], R> recordMapper;

        /**
         * Alternative to {@link CSVReader.Builder#mapped(TypeToken, boolean)}, identical functioning
         *
         * @param readerBuilder CSVReader configuration to use
         * @param recordType    Record type to construct
         * @param readHeader    If true, the first line of the document will be parsed as a header. If false, first line of document will be parsed as data.
         */
        // Design choice: We include the readHeader so that no other configuration methods are required for a 'default behaviour' mapper
        public Builder(CSVReader.Builder readerBuilder, TypeToken<R> recordType, boolean readHeader) {
            this.readerBuilder = Objects.requireNonNull(readerBuilder).validate().copySettings();   // Make a copy to ensure mutations of `readerBuilder` do not impact this CSVMapper.Builder, to avoid unexpected behaviour
            this.recordType = Objects.requireNonNull(recordType);
            this.readHeader = readHeader;
            this.ignoreExcessColumns = true;
            this.inferEmptyTrailingColumns = false;
            this.headerCompareFunction = String::equalsIgnoreCase;
            this.typeMappers = new HashMap<>(DEFAULT_TYPE_MAPPERS);
            this.isValidated = false;
            if (!recordType.isRecord()) throw new IllegalArgumentException("specified type must be a record, found: " + recordType);
        }

        /**
         * @param readHeader If true, the first line of the document will be parsed as a header. If false, first line of document will be parsed as data.
         * @return this builder, for chaining
         */
        public Builder<R> readHeader(boolean readHeader) {
            this.readHeader = readHeader;
            this.isValidated = false;
            return this;
        }

        /**
         * If set to true, CSVMapper will ignore excess columns. If set to false, CSVMapper will throw an exception if a CSV row has more columns than expected
         * <br>Default: True
         * <br>Example<br>
         * <pre>
         * True:
         * ✔️ `value1,value2,value3` -> Record3("value1", "value2", "value3")
         * ✔️ `value1,value2,value3,value4` -> Record3("value1", "value2", "value3")
         * False:
         * ✔️ `value1,value2,value3` -> Record3("value1", "value2", "value3")
         * ❌ `value1,value2,value3,value4` -> Exception
         * </pre>
         *
         * @param ignoreExcessColumns configuration value
         * @return this builder, for chaining
         */
        public Builder<R> ignoreExcessColumns(boolean ignoreExcessColumns) {
            this.ignoreExcessColumns = ignoreExcessColumns;
            this.isValidated = false;
            return this;
        }

        /**
         * If set to true, CSVMapper will fill missing columns with empty strings. If set to false, CSVMapper will throw an exception for missing columns
         * <br>Default: False
         * <br>
         * NOTE: Conversion to any type other than String will still likely fail
         * <br>Example<br>
         * <pre>
         * True:
         * ✔️ `value1,value2,value3` -> Record3("value1", "value2", "value3")
         * ✔️ `value1,value2` -> Record3("value1", "value2", "")
         * False:
         * ✔️ `value1,value2,value3` -> Record3("value1", "value2", "value3")
         * ❌ `value1,value2` -> Exception
         * </pre>
         *
         * @param inferEmptyTrailingColumns configuration value
         * @return this builder, for chaining
         */
        public Builder<R> inferEmptyTrailingColumns(boolean inferEmptyTrailingColumns) {
            this.inferEmptyTrailingColumns = inferEmptyTrailingColumns;
            this.isValidated = false;
            return this;
        }

        /**
         * Sets function used to compare header fields for equality
         * <br>Default: {@link String#equalsIgnoreCase(String) String::equalsIgnoreCase}
         * <br>Example<br>
         * <pre>
         * String::equals:
         * ✔️ `one,two,three` -> Record3(String one, String two, String three)
         * ❌ `One,Two,Three` -> Exception
         * String::equalsIgnoreCase:
         * ✔️ `one,two,three` -> Record3(String one, String two, String three)
         * ✔️ `One,Two,Three` -> Record3(String one, String two, String three)
         * </pre>
         *
         * @param headerCompareFunction configuration value
         * @return this builder, for chaining
         * @throws NullPointerException if headerCompareFunction is null
         */
        public Builder<R> setHeaderCompareFunction(@NotNull BiFunction<String, String, Boolean> headerCompareFunction) {
            this.headerCompareFunction = Objects.requireNonNull(headerCompareFunction);
            this.isValidated = false;
            return this;
        }

        /**
         * Adds a type mapper to this builder.
         * <br>Type mappers convert the string parsed from the csv document to the type of the record field
         * <br>Note: TypeToken must be exact to the type used in the record
         * <br>I.e. `List&lt;Integer&gt;`, `List&lt;? extends Integer&gt;`, `List&lt;?&gt;` are each different types
         * <br>
         * <br>For defaults see {@link CSVMapper.Builder#DEFAULT_TYPE_MAPPERS}
         *
         * @param type   Type for which to add a mapper
         * @param mapper Mapper function
         * @return this builder, for chaining
         * @throws NullPointerException if either type or mapper are null
         */
        @SuppressWarnings("unchecked")  // We cast <T> to Object, this is always safe
        public <T> Builder<R> addTypeMapper(TypeToken<T> type, ThrowingFunction<String, T> mapper) {
            this.typeMappers.put(Objects.requireNonNull(type), (ThrowingFunction<String, Object>) Objects.requireNonNull(mapper));
            this.isValidated = false;
            return this;
        }


        /**
         * Adds multiple type mappers to this builder, see {@link CSVMapper.Builder#addTypeMapper(TypeToken, ThrowingFunction)}
         * <br>Type mappers convert the string parsed from the csv document to the type of the record field
         * <br>Note: TypeToken must be exact to the type used in the record
         * <br>I.e. `List&lt;Integer&gt;`, `List&lt;? extends Integer&gt;`, `List&lt;?&gt;` are each different types
         * <br>
         * <br>For defaults see {@link CSVMapper.Builder#DEFAULT_TYPE_MAPPERS}
         *
         * @param mappers Mapper functions
         * @return this builder, for chaining
         * @throws NullPointerException if either type or mapper are null
         */
        public Builder<R> addTypeMappers(Map<TypeToken<?>, ThrowingFunction<String, Object>> mappers) {
            this.typeMappers.putAll(Util.requireNonNullEntries(mappers));
            this.isValidated = false;
            return this;
        }

        /**
         * Validates configuration, ensuring any future calls to {@link CSVMapper.Builder#build(BufferedReader)} throw no error
         * <br>
         * This method performs reflection lookup, and may start throwing exceptions as a result of changes to the Record which the CSVMapper instantiates
         * <br>
         * The purpose of this method is to provide fail-fast behaviour when reusing builders
         *
         * @return this builder, for assigning to a variable
         * @throws IllegalStateException If the current builder configuration is invalid
         */
        public Builder<R> validate() throws IllegalStateException {
            TypeToken.RecordField[] fields = recordType.getRecordComponents();
            String[] csvHeader = new String[fields.length];
            @SuppressWarnings("unchecked")
            ThrowingFunction<String, Object>[] fieldMappers = new ThrowingFunction[fields.length];
            for (int i = 0; i < fields.length; i++) {
                csvHeader[i] = fields[i].name();
                fieldMappers[i] = this.typeMappers.get(fields[i].type());
                if (fieldMappers[i] == null) throw new IllegalStateException("no mapper found for type " + fields[i].type());
            }

            this.csvHeader = csvHeader;
            this.fieldMappers = fieldMappers;

            try {
                this.recordMapper = recordType.getRecordConstructor();
            } catch (NoSuchMethodException e) { // Change exception type to IllegalStateException
                throw new IllegalStateException(e.getMessage());
            }

            this.isValidated = true;
            return this;
        }

        /**
         * Builds a new mapper for the given input
         * <br>
         * May be called multiple times to create new mappers with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVMapper that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVMapper
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVMapper<R> build(BufferedReader input) throws IOException, IllegalStateException {
            Objects.requireNonNull(input);
            if (!this.isValidated) this.validate();

            return new CSVMapper<>(
                    readerBuilder.build(input),
                    readHeader,
                    ignoreExcessColumns,
                    inferEmptyTrailingColumns,
                    headerCompareFunction,
                    csvHeader, fieldMappers, recordMapper
            );
        }

        /**
         * Builds a new mapper for the given input
         * <br>
         * May be called multiple times to create new mappers with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVMapper that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVMapper
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVMapper<R> build(Reader input) throws IOException {
            Objects.requireNonNull(input);
            if (input instanceof BufferedReader bufferedReader) {
                return build(bufferedReader);
            } else {
                return build(new BufferedReader(input));
            }
        }

        /**
         * Builds a new mapper for the given input
         * <br>
         * May be called multiple times to create new mappers with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVMapper that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVMapper
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVMapper<R> build(String input) throws IOException {
            Objects.requireNonNull(input);
            return this.build(new BufferedReader(new StringReader(input)));
        }
    }
}

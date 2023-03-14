package net.sentientturtle.csv;

import net.sentientturtle.csv.reflection.TypeToken;
import net.sentientturtle.csv.util.Util;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * CSVReader
 * <br>
 * Instantiated through the {@link CSVReader.Builder#mapped(TypeToken, boolean)} factory method.
 * <br>
 * Implements {@link Iterable} and can be used in for-each loops.
 * <br>
 * CAUTION: CSVReader can only be iterated once. Creating a new iterator ({@link CSVMapper#iterator()}) does not reset iterator state, iteration will continue where the previous iterator left. To iterate multiple times, collect each row to a list.
 * <br><br>
 * Example usage:
 * <pre>
 * try (
 *         CSVReader csv = CSVReader.Builder.COMMA_SEPARATED()
 *                                 .build(new FileReader("./file.csv"))
 * ) {
 *     for (String[] row : csv) {
 *         // Use row
 *     }
 * } catch (IOException | CSVParseException error) {
 *     // Handle IO or parsing error
 * }
 * </pre>
 */
public class CSVReader implements Iterable<String[]>, AutoCloseable {
    // Input
    private final BufferedReader reader;
    // Configuration
    private final int unitSeparator;
    private final int recordSeparator;
    private final boolean trimWhitespace;
    private final QuoteParsingMode quoteMode;
    private final Function<Integer, Boolean> isWhitespace;
    // Buffers
    private final StringBuilder unitBuilder;
    private final ArrayList<String> lineBuffer;
    // State
    private volatile boolean hasNext;
    /**
     * Amount of successfully read rows, incremented at the start of {@link #readLine()} such that it refers to the line currently being read when reading is in progress.
     */
    private int rowCount;

    /**
     * Private constructor, this type is initialized through {@link CSVReader.Builder}
     * <br>
     * Caution: Performs an initial read of the input to initialize iterator state, may block or throw an exception
     *
     * @param reader          CSV document input
     * @param unitSeparator   Separator character for units/values (Specified as codepoint integer)
     * @param recordSeparator Separator character for records/lines (Specified as codepoint integer)
     * @param trimWhitespace  True -> Trim whitespace, False -> Leave whitespace
     * @param isWhitespace    Function used to determine whitespace, takes codepoint integers. Usually {@link Character#isWhitespace(int) Character::isWhitespace}
     * @param quoteMode       Quote parsing mode, see {@link QuoteParsingMode} for details
     * @throws IOException If an error occurs during initial read
     */
    private CSVReader(
            BufferedReader reader,
            int unitSeparator,
            int recordSeparator,
            boolean trimWhitespace,
            Function<Integer, Boolean> isWhitespace,
            QuoteParsingMode quoteMode
    ) throws IOException {
        this.reader = reader;
        this.unitSeparator = unitSeparator;
        this.recordSeparator = recordSeparator;
        this.trimWhitespace = trimWhitespace;
        this.quoteMode = quoteMode;
        this.isWhitespace = isWhitespace;

        unitBuilder = new StringBuilder();  // TODO: Size hinting
        lineBuffer = new ArrayList<>();

        // Peek reader, set hasNext to false if we are already at end of stream.
        this.reader.mark(1);
        this.hasNext = this.reader.read() != -1;
        this.reader.reset();
        rowCount = 0;
    }

    /**
     * @return Single codepoint
     * @throws IOException       If an IO error occurs while reading from input
     * @throws CSVParseException If malformed unicode (invalid surrogate pair) is encountered
     */
    private int readCodepoint() throws IOException, CSVParseException {
        int character = reader.read();
        if (character == -1) {
            return character;
        } else if (Character.isHighSurrogate((char) character)) {
            int lowSurrogate = reader.read();
            if (lowSurrogate == -1) throw new CSVParseException("end-of-stream after high surrogate character");
            if (Character.isLowSurrogate((char) lowSurrogate)) {
                return Character.toCodePoint((char) character, (char) lowSurrogate);
            } else {
                throw new CSVParseException("missing low surrogate character");
            }
        } else if (Character.isLowSurrogate((char) character)) {
            throw new CSVParseException("unexpected low surrogate character");
        } else {
            return character;
        }
    }

    /**
     * Read a single line from the CSV document
     * <br>
     * Use of {@link CSVReader#iterator()}, {@link CSVReader#spliterator()}, or {@link CSVReader#stream(boolean)} recommended for better handling of end-of-file
     *
     * @return Record representing the next
     * @throws NoSuchElementException If end-of-file has been reached
     * @throws IOException            If an error occurs while reading input
     * @throws NoSuchElementException If end-of-stream has been reached
     * @throws CSVParseException      If a CSV parsing exception occurs
     */
    public synchronized String[] readLine() throws IOException, NoSuchElementException, CSVParseException {
        assert lineBuffer.size() == 0;
        this.rowCount += 1;

        int codepoint = readCodepoint();

        if (codepoint == -1) throw new NoSuchElementException("end of stream reached trying to read row " + this.rowCount);

        while (codepoint != -1 && codepoint != recordSeparator) {
            assert unitBuilder.length() == 0;

            if (trimWhitespace) while (codepoint != -1 && codepoint != unitSeparator && codepoint != recordSeparator && isWhitespace.apply(codepoint)) {
                codepoint = readCodepoint();    // Skip codepoint
            }

            if (codepoint == '"' && quoteMode != QuoteParsingMode.TREAT_QUOTES_AS_NORMAL_CHARACTERS) { // Unit in quotes
                read_to_unit_end:
                while (true) {
                    codepoint = readCodepoint();
                    // Read to next quote (Or end of stream)
                    while (codepoint != '"' && codepoint != -1) {
                        unitBuilder.appendCodePoint(codepoint);
                        codepoint = readCodepoint();
                    }

                    if (codepoint == -1) throw new IOException("unclosed quotes in row " + rowCount);

                    codepoint = readCodepoint();
                    if (codepoint == '"') {
                        unitBuilder.append('"');
                        continue read_to_unit_end;
                    } else {
                        break read_to_unit_end;
                    }
                }

                // Trim whitespace now, as it is an error to have further (non whitespace) characters after the closing of the quoted string
                if (trimWhitespace) while (codepoint != -1 && codepoint != unitSeparator && codepoint != recordSeparator && isWhitespace.apply(codepoint)) {
                    codepoint = readCodepoint();    // Skip codepoint
                }
                if (codepoint != -1 && codepoint != unitSeparator && codepoint != recordSeparator) {
                    throw new CSVParseException("continuation character (" + Character.toString(codepoint) + ") after end of quoted block, in row " + rowCount);
                }
            } else {
                while (codepoint != -1 && codepoint != unitSeparator && codepoint != recordSeparator) {
                    if (quoteMode == QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS && codepoint == '"') throw new CSVParseException("double-quote mark in non-quoted block, in row " + rowCount);
                    unitBuilder.appendCodePoint(codepoint);
                    codepoint = readCodepoint();
                }
                if (trimWhitespace) {
                    trim_trailing_whitespace:
                    while (unitBuilder.length() > 0) {
                        char character = unitBuilder.charAt(unitBuilder.length() - 1);
                        if (Character.isLowSurrogate(character) && unitBuilder.length() >= 2) {
                            int lastCodepoint = unitBuilder.codePointAt(unitBuilder.length() - 2);
                            if (isWhitespace.apply(lastCodepoint)) {
                                unitBuilder.setLength(unitBuilder.length() - 2);
                            } else {
                                break trim_trailing_whitespace;
                            }
                        } else {
                            if (isWhitespace.apply((int) character)) {
                                unitBuilder.setLength(unitBuilder.length() - 1);
                            } else {
                                break trim_trailing_whitespace;
                            }
                        }
                    }
                }
            }

            if (codepoint == unitSeparator) codepoint = readCodepoint();

            if (codepoint == -1) {
                hasNext = false;
            }

            if (codepoint == '\n' && unitBuilder.charAt(unitBuilder.length() - 1) == '\r') {    // Remove \r\n newlines outside trim-whitespace mode.
                unitBuilder.setLength(unitBuilder.length() - 1);
            }
            String unit = unitBuilder.toString();
            unitBuilder.setLength(0);
            lineBuffer.add(unit);
        }

        String[] array = lineBuffer.toArray(String[]::new);
        lineBuffer.clear();
        return array;
    }

    /**
     * @return True if this CSVReader has not yet encountered end-of-file, and another line may be read.
     */
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * Use this CSVReader as an Iterator
     * <br><br>
     * CAUTION: CSVReader can only be iterated once. Creating a new iterator does not reset iteration state, iterator will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" iterator, which may throw {@link IOException} or {@link CSVParseException} if an IO or parsing error occurs
     *
     * @return Iterator equivalent of this CSVReader.
     */
    @NotNull
    @Override
    public Iterator<String[]> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return CSVReader.this.hasNext;
            }

            @Override
            public String[] next() {
                try {
                    return CSVReader.this.readLine();
                } catch (IOException | CSVParseException e) {
                    return Util.sneakyThrow(e);
                }
            }
        };
    }

    /**
     * Use this CSVReader as a Spliterator
     * <br><br>
     * CAUTION: CSVReader can only be iterated once. Creating a new spliterator does not reset iteration state, spliterator will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" spliterator, which may throw {@link IOException} or {@link CSVParseException} if an IO or parsing error occurs
     *
     * @return Spliterator equivalent of this CSVReader.
     */
    @Override
    public Spliterator<String[]> spliterator() {
        return Spliterators.spliteratorUnknownSize(
                this.iterator(),
                Spliterator.ORDERED | Spliterator.NONNULL
        );
    }

    /**
     * Use this CSVReader as a Stream
     * <br><br>
     * CAUTION: CSVReader can only be iterated once. Creating a new stream does not reset iteration state, stream will continue where the previous iteration left. To iterate multiple times, collect each record to a list.
     * <br><br>
     * WARNING: This is a "throwing" stream, which may throw {@link IOException} or {@link CSVParseException} if an IO or parsing error occurs
     *
     * @param parallel if true, return a parallel stream
     * @return Stream equivalent of this CSVReader.
     */
    public Stream<String[]> stream(boolean parallel) {
        return StreamSupport.stream(spliterator(), parallel);
    }

    /**
     * Closes this CSVReader; Closing its input reader and preventing future reads
     * <br>
     * If this reader is used as an iterator or (spliterator/stream), that iterator will no longer yield any csv rows
     *
     * @throws IOException If an error occurs closing the backing reader
     */
    @Override
    @SuppressWarnings("RedundantThrows")    // We do not throw CSVParseException here, but it may be silently thrown by iteration. Inclusion here ensures a catch clause is present to handle exceptions during iteration
    public synchronized void close() throws IOException, CSVParseException {
        this.hasNext = false;
        this.reader.close();
    }

    /**
     * @return unit/value separator codepoint used for parsing CSV documents
     */
    public int getUnitSeparator() {
        return unitSeparator;
    }

    /**
     * @return record/line separator codepoint used for parsing CSV documents
     */
    public int getRecordSeparator() {
        return recordSeparator;
    }

    /**
     * @return Amount of successfully read rows
     */
    public int rowCount() {
        return rowCount;
    }


    /**
     * Builder for {@link CSVReader}
     * <br>
     * Multiple default configurations are provided
     * <br>
     * See method documentation for details on configuration
     * <br>
     * Note: This builder may be used to create multiple readers. {@link CSVMapper.Builder#validate()} may be used for fail-fast configuration validation
     *
     * @param <R> Type of record provided by this mapper
     */
    public static class Builder {
        private @Nullable Integer unitSeparator;
        private @Nullable Integer recordSeparator;
        private @Nullable Boolean trimWhitespace;
        private @Nullable QuoteParsingMode quoteMode;
        private @NotNull Function<Integer, Boolean> isWhitespace;

        /**
         * Create a builder with empty configuration; All configuration options must manually be set before {@link Builder#build(String)}
         */
        public Builder() {
            this.isWhitespace = Character::isWhitespace;
        }

        /**
         * Sets character used to separate value/units
         * <br>
         * This implementation only supports a single unit separator character
         *
         * @param separator value/unit separator character
         * @return this builder, for chaining
         */
        public Builder setUnitSeparator(int separator) {
            this.unitSeparator = separator;
            return this;
        }

        /**
         * Sets character used to separate rows/lines
         * <br>
         * This implementation only supports a single record separator character
         * <br>
         * NOTE: A special case is made for `\n`, where both '\n' and '\r\n' are treated as the separator.
         *
         * @param separator row/line separator character
         * @return this builder, for chaining
         */
        public Builder setRecordSeparator(int separator) {
            this.recordSeparator = separator;
            return this;
        }

        /**
         * If true, trim whitespace from values
         * <br>
         * Which codepoints are deemed whitespace can be configured with {@link Builder#setWhitespaceDefinition(Function)}, default {@link Character#isWhitespace(int)}
         *
         * @param trimWhitespace if true, trim whitespace
         * @return this builder, for chaining
         */
        public Builder trimWhitespace(boolean trimWhitespace) {
            this.trimWhitespace = trimWhitespace;
            return this;
        }

        /**
         * Sets definition of whitespace for built CSVReader
         *
         * @param isCodepointWhitespace function mapping codepoint integer to true if codepoint is whitespace, to false otherwise
         * @return this builder, for chaining
         */
        public Builder setWhitespaceDefinition(@NotNull Function<Integer, Boolean> isCodepointWhitespace) {
            this.isWhitespace = Objects.requireNonNull(isCodepointWhitespace);
            return this;
        }

        /**
         * Sets quote parsing mode; Determining how CSVReader handles double-quote characters
         * <br>
         * See {@link QuoteParsingMode} for details on what each option does
         *
         * @param quoteMode Quote parsing mode to use for built CSVReader
         * @return this builder, for chaining
         */
        public Builder setQuoteMode(@NotNull CSVReader.QuoteParsingMode quoteMode) {
            this.quoteMode = Objects.requireNonNull(quoteMode);
            return this;
        }

        /**
         * Validates configuration, ensuring any future calls to {@link Builder#build(BufferedReader)} throw no error
         * <br>
         * The purpose of this method is to provide fail-fast behaviour when reusing builders
         *
         * @return this builder, for assigning to a variable
         * @throws IllegalStateException If the current builder configuration is invalid
         */
        public Builder validate() throws IllegalStateException {
            if (this.unitSeparator == null) throw new IllegalStateException("unit separator not set");
            if (this.recordSeparator == null) throw new IllegalStateException("record separator not set");
            if (this.trimWhitespace == null) throw new IllegalStateException("trim-whitespace option not set");
            if (this.quoteMode == null) throw new IllegalStateException("quote parsing mode not set");
            return this;
        }

        /**
         * Builds a new CSVReader for the given input
         * <br>
         * May be called multiple times to create new CSVReaders with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVReader that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVReader
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVReader build(BufferedReader input) throws IOException {
            validate();
            //noinspection DataFlowIssue    Linter cannot see that #validate() null-checks
            return new CSVReader(Objects.requireNonNull(input), unitSeparator, recordSeparator, trimWhitespace, isWhitespace, quoteMode);
        }

        /**
         * Builds a new CSVReader for the given input
         * <br>
         * May be called multiple times to create new CSVReaders with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVReader that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVReader
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVReader build(Reader input) throws IOException {
            Objects.requireNonNull(input);
            if (input instanceof BufferedReader bufferedReader) {
                return build(bufferedReader);
            } else {
                return build(new BufferedReader(input));
            }
        }

        /**
         * Builds a new CSVReader for the given input
         * <br>
         * May be called multiple times to create new CSVReaders with the same configuration
         * <br>
         * Validates configuration, for fail-fast behaviour when reusing builders, call {@link Builder#validate()}
         * <br>
         * NOTE: Performs an initial (blocking) read of the input
         *
         * @param input Input CSV document
         * @return CSVReader that yields rows from the input document
         * @throws IOException           if an IOException occurs initialising the CSVReader
         * @throws IllegalStateException if configuration is invalid
         */
        public CSVReader build(String input) throws IOException {
            return this.build(new BufferedReader(new StringReader(Objects.requireNonNull(input))));
        }

        /**
         * Creates a builder for CSVMapper, mapping CSV lines to specified record type
         * <br>Uses current configuration. Any changes made to this (Reader builder) after this call, will not apply to the returned Mapper builder
         *
         * @param recordType TypeToken representing record to map to
         * @param hasHeader  true if the CSV document has a header
         * @param <R>        type of record
         * @return CSVMapper builder
         */
        public <R extends Record> CSVMapper.Builder<R> mapped(TypeToken<R> recordType, boolean hasHeader) {
            this.validate();
            return new CSVMapper.Builder<>(this, recordType, hasHeader);
        }

        /**
         * Creates a builder for CSVMapper, mapping CSV lines to specified record type
         * <br>Uses current configuration. Any changes made to this (Reader builder) after this call, will not apply to the returned Mapper builder
         * <br>Warning: This method only works with non-generic classes. Use {@link Builder#mapped(TypeToken, boolean)} if your record is generic
         *
         * @param recordType class representing record to map to
         * @param hasHeader  true if the CSV document has a header
         * @param <R>        type of record
         * @return CSVMapper builder
         */
        public <R extends Record> CSVMapper.Builder<R> mapped(Class<R> recordType, boolean hasHeader) {
            this.validate();
            if (recordType.getTypeParameters().length > 0) throw new IllegalArgumentException("Cannot resolve record type: Types with generic parameters must use #mapped(TypeToken, boolean)");
            return new CSVMapper.Builder<>(this, new TypeToken<>(recordType), hasHeader);
        }

        /**
         * Creates a new builder with the same settings, that may be independently modified
         * <br>Note: This is a shallow copy, copy use the same instance of white space definition ({@link Builder#setWhitespaceDefinition(Function)})
         *
         * @return A copy of this builder with identical settings
         */
        public CSVReader.Builder copySettings() {
            Builder copy = new CSVReader.Builder();
            copy.unitSeparator = this.unitSeparator;
            copy.recordSeparator = this.recordSeparator;
            copy.trimWhitespace = this.trimWhitespace;
            copy.quoteMode = this.quoteMode;
            copy.isWhitespace = this.isWhitespace;
            return copy;
        }

        /**
         * @return Default configuration matching RFC4180 CSV parsing behaviour
         * <br>No whitespace trimming
         */
        public static Builder DEFAULT_RFC4180() {
            return new Builder()
                           .setUnitSeparator(',')
                           .setRecordSeparator('\n')
                           .trimWhitespace(false)
                           .setWhitespaceDefinition(Character::isWhitespace)
                           .setQuoteMode(QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
        }


        /**
         * @return Default configuration matching comma-separated-values
         * <br>No whitespace trimming
         * <br>Identical to {@link Builder#DEFAULT_RFC4180()}
         */
        public static Builder COMMA_SEPARATED_STRICT() {
            return DEFAULT_RFC4180();
        }

        /**
         * @return Default configuration matching comma-separated-values
         * <br>Whitespace trimming according to {@link Character#isWhitespace(int)}
         */
        public static Builder COMMA_SEPARATED_TRIM_WHITESPACE() {
            return new Builder()
                           .setUnitSeparator(',')
                           .setRecordSeparator('\n')
                           .trimWhitespace(true)
                           .setWhitespaceDefinition(Character::isWhitespace)
                           .setQuoteMode(QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
        }

        /**
         * @return Default configuration matching tab-separated-values
         * <br>Whitespace trimming according to {@link Character#isWhitespace(int)}
         */
        public static Builder TAB_SEPARATED() {
            return new Builder()
                           .setUnitSeparator('\t')
                           .setRecordSeparator('\n')
                           .trimWhitespace(true)
                           .setWhitespaceDefinition(Character::isWhitespace)
                           .setQuoteMode(QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
        }


        /**
         * @return Default configuration using ascii separator characters (U+001F and U+001E)
         * <br>No whitespace trimming
         * <br>Double-quotes are treated as regular characters
         */
        public static Builder ASCII_SEPARATED() {
            return new Builder()
                           .setUnitSeparator('\u001F')
                           .setRecordSeparator('\u001E')
                           .trimWhitespace(false)
                           .setWhitespaceDefinition(Character::isWhitespace)
                           .setQuoteMode(QuoteParsingMode.TREAT_QUOTES_AS_NORMAL_CHARACTERS);
        }
    }

    /**
     * Options for quote parsing behaviour
     */
    public enum QuoteParsingMode {
        /**
         * Treat quotation marks as normal characters, <b>{@code "VALUE"}</b> is parsed to <b>{@code "VALUE"}</b>
         * <br>
         * Fields cannot contain special characters in this parsing mode
         */
        TREAT_QUOTES_AS_NORMAL_CHARACTERS,
        /**
         * Treat quotation marks as special characters, <b>{@code "VALUE"}</b> is parsed to <b>{@code VALUE}</b>
         * <br>
         * Additionally, permit 'inner' quotation marks, such as <b>{@code HELLO"WORLD}</b>, parsed to <b>{@code HELLO"WORLD}</b>
         */
        PERMIT_INNER_QUOTES_IN_UNQUOTED_FIELDS,
        /**
         * Treat quotation marks as special characters, <b>{@code "VALUE"}</b> is parsed to <b>{@code VALUE}</b>
         * <br>
         * Does not permit 'inner' quotation marks such as <b>{@code HELLO"WORLD}</b>, an exception is raised if encountered
         * <br>
         * Follows RFC-4180 CSV format
         */
        REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS
    }
}

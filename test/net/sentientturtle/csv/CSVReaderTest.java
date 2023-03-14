package net.sentientturtle.csv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Tests for {@link CSVReader}
 * <br>
 * NOTE: These tests are used by {@link CSVMapperTest}, if tests both here and in {@link CSVMapperTest} fail, the former should be resolved first
 */
public class CSVReaderTest {
    private static final String HEADER = "header1,header2,header3,header4";
    private static final String VALUES = "value1,value2,value3,value4";
    private static final String CHARACTER_OUTSIDE_BMP = "value \uD83D\uDE0A,\"value \uD83D\uDE0A\", value \uD83D\uDE0A ";

    private static final String ADDED_WHITESPACE = "value 1, value 2,value 3 , value 4 ";

    private static final String QUOTED_VALUES = "value 1,\"value 2\",\"value\"\"3\",\"value\"\"\"\"\"\"4\"";
    private static final String SPECIAL_CHARACTERS_IN_QUOTED_VALUES = "\"value,\n\"";
    private static final String LEADING_WHITESPACE_BEFORE_QUOTED_VALUE = " \"value\"";
    private static final String TRAILING_WHITESPACE_AFTER_QUOTED_VALUE = "\"value\" ";
    private static final String INNER_QUOTE_IN_UNQUOTED_FIELD = "HELLO\"WORLD,HELLO\"WORLD";

    private static final String LEADING_AND_TRAILING_UNDERSCORE = "__value1,value2__,  value3__  ";

    private static final String TAB_SEPARATOR_HEADER = "header1\theader2\theader3\theader4";
    private static final String TAB_SEPARATOR_VALUES = "value1\tvalue2\tvalue3\tvalue4";
    private static final String TAB_SEPARATOR_ADDED_WHITESPACE = "value 1\t value 2\tvalue 3 \t value 4 ";

    private static final String ASCII_SEPARATOR_HEADER = "header1\u001Fheader2\u001Fheader3\u001Fheader4";
    private static final String ASCII_SEPARATOR_VALUES = "value1\u001Fvalue2\u001Fvalue3\u001Fvalue4";

    // Malformed data
    private static final String INVALID_UNICODE_MISSING_LOW_SURROGATE = "value \uD83D,value 2";
    private static final String INVALID_UNICODE_MISSING_LOW_SURROGATE_EOF = "value \uD83D";
    private static final String INVALID_UNICODE_MISSING_HIGH_SURROGATE = "value 1 \uDE0A, value 2";

    private void assertCSVLineEquals(CSVReader.Builder builder, String line, String... expectedValues) throws IOException, CSVParseException {
        try (CSVReader reader = builder.build(line)) {
            Assertions.assertArrayEquals(
                    expectedValues,
                    reader.readLine()
            );
            Assertions.assertFalse(reader.iterator().hasNext());
        }
    }

    private CSVReader.Builder createTestReader() {
        return new CSVReader.Builder()
                       .setUnitSeparator(',')
                       .setRecordSeparator('\n')
                       .trimWhitespace(false)
                       .setWhitespaceDefinition(Character::isWhitespace)
                       .setQuoteMode(CSVReader.QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
    }

    @Test
    public void basicCSV() throws IOException, CSVParseException {
        CSVReader.Builder builder = createTestReader();

        assertCSVLineEquals(builder, VALUES, "value1", "value2", "value3", "value4");
    }

    @Test
    public void textOutsideBMP() throws IOException, CSVParseException {
        CSVReader.Builder builder = createTestReader().trimWhitespace(true);

        assertCSVLineEquals(builder, CHARACTER_OUTSIDE_BMP, "value \uD83D\uDE0A", "value \uD83D\uDE0A", "value \uD83D\uDE0A");

        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(builder, INVALID_UNICODE_MISSING_LOW_SURROGATE, "value \uD83D", "value 2"));
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(builder, INVALID_UNICODE_MISSING_LOW_SURROGATE_EOF, "value \uD83D"));
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(builder, INVALID_UNICODE_MISSING_HIGH_SURROGATE, "value 1 \uDE0A", "value 2"));
    }

    @Test
    public void separators() throws IOException, CSVParseException {
        List<List<String>> expected = List.of(
                List.of("header1", "header2", "header3", "header4"),
                List.of("value1", "value2", "value3", "value4"),
                List.of("value1", "value2", "value3", "value4")
        );

        CSVReader comma = createTestReader()
                                  .setUnitSeparator(',')
                                  .setRecordSeparator('\n')
                                  .build(HEADER + "\n" + VALUES + "\r\n" + VALUES);

        Assertions.assertIterableEquals(expected, comma.stream(false).map(List::of).toList());  // Map arrays to list for value-equality

        CSVReader tab = createTestReader()
                                .setUnitSeparator('\t')
                                .setRecordSeparator('\n')
                                .build(TAB_SEPARATOR_HEADER + "\n" + TAB_SEPARATOR_VALUES + "\r\n" + TAB_SEPARATOR_VALUES);

        Assertions.assertIterableEquals(expected, tab.stream(false).map(List::of).toList());

        CSVReader asciiSeparator = createTestReader()
                                           .setUnitSeparator('\u001F')
                                           .setRecordSeparator('\u001E')
                                           .build(ASCII_SEPARATOR_HEADER + "\u001E" + ASCII_SEPARATOR_VALUES + "\u001E" + ASCII_SEPARATOR_VALUES);

        Assertions.assertIterableEquals(expected, asciiSeparator.stream(false).map(List::of).toList());  // Map arrays to list for value-equality
    }

    @Test
    public void whitespace() throws IOException, CSVParseException {
        CSVReader.Builder noTrim = createTestReader().trimWhitespace(false);
        assertCSVLineEquals(noTrim, ADDED_WHITESPACE, "value 1", " value 2", "value 3 ", " value 4 ");

        CSVReader.Builder yesTrim = createTestReader().trimWhitespace(true);
        assertCSVLineEquals(yesTrim, ADDED_WHITESPACE, "value 1", "value 2", "value 3", "value 4");

        CSVReader.Builder customTrim = createTestReader().trimWhitespace(true).setWhitespaceDefinition(codepoint -> codepoint == (int) '_');
        assertCSVLineEquals(customTrim, LEADING_AND_TRAILING_UNDERSCORE, "value1", "value2", "  value3__  ");

        CSVReader.Builder trimAndTabSeparator = createTestReader()
                                                        .trimWhitespace(true)
                                                        .setUnitSeparator('\t');
        assertCSVLineEquals(trimAndTabSeparator, TAB_SEPARATOR_ADDED_WHITESPACE, "value 1", "value 2", "value 3", "value 4");
    }

    @Test
    public void quotationmarks() throws IOException, CSVParseException {
        CSVReader.Builder rejectInnerQuotes = createTestReader()
                                                      .setQuoteMode(CSVReader.QuoteParsingMode.REJECT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
        assertCSVLineEquals(rejectInnerQuotes, QUOTED_VALUES, "value 1", "value 2", "value\"3", "value\"\"\"4");
        assertCSVLineEquals(rejectInnerQuotes, SPECIAL_CHARACTERS_IN_QUOTED_VALUES, "value,\n");
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(rejectInnerQuotes, LEADING_WHITESPACE_BEFORE_QUOTED_VALUE, "value"));
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(rejectInnerQuotes, TRAILING_WHITESPACE_AFTER_QUOTED_VALUE, "value"));
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(rejectInnerQuotes, INNER_QUOTE_IN_UNQUOTED_FIELD, "HELLO\"WORLD", "HELLO\"WORLD"));

        CSVReader.Builder permitInnerQuotes = createTestReader()
                                                      .setQuoteMode(CSVReader.QuoteParsingMode.PERMIT_INNER_QUOTES_IN_UNQUOTED_FIELDS);
        assertCSVLineEquals(permitInnerQuotes, QUOTED_VALUES, "value 1", "value 2", "value\"3", "value\"\"\"4");
        assertCSVLineEquals(permitInnerQuotes, SPECIAL_CHARACTERS_IN_QUOTED_VALUES, "value,\n");
        assertCSVLineEquals(permitInnerQuotes, LEADING_WHITESPACE_BEFORE_QUOTED_VALUE, " \"value\"");
        assertCSVLineEquals(permitInnerQuotes, INNER_QUOTE_IN_UNQUOTED_FIELD, "HELLO\"WORLD", "HELLO\"WORLD");
        Assertions.assertThrows(CSVParseException.class, () -> assertCSVLineEquals(permitInnerQuotes, TRAILING_WHITESPACE_AFTER_QUOTED_VALUE, "value"));

        CSVReader.Builder ignoreQuotes = createTestReader()
                                                 .setQuoteMode(CSVReader.QuoteParsingMode.TREAT_QUOTES_AS_NORMAL_CHARACTERS);
        assertCSVLineEquals(ignoreQuotes, QUOTED_VALUES, "value 1", "\"value 2\"", "\"value\"\"3\"", "\"value\"\"\"\"\"\"4\"");
        List<List<String>> expected = List.of(
                List.of("\"value"),
                List.of("\"")
        );
        Assertions.assertIterableEquals(expected, ignoreQuotes.build(SPECIAL_CHARACTERS_IN_QUOTED_VALUES).stream(false).map(List::of).toList());  // Map arrays to list for value-equality

        assertCSVLineEquals(ignoreQuotes, LEADING_WHITESPACE_BEFORE_QUOTED_VALUE, " \"value\"");
        assertCSVLineEquals(ignoreQuotes, INNER_QUOTE_IN_UNQUOTED_FIELD, "HELLO\"WORLD", "HELLO\"WORLD");
        assertCSVLineEquals(ignoreQuotes, TRAILING_WHITESPACE_AFTER_QUOTED_VALUE, "\"value\" ");
    }


    private final BufferedReader endOfStreamReader = new BufferedReader(new StringReader(""));

    @Test
    public void defaultBuildersAreValid() throws IOException {
        // When other default configurations are added,
        CSVReader.Builder.DEFAULT_RFC4180()
                .validate()
                .build(endOfStreamReader);

        CSVReader.Builder.COMMA_SEPARATED_STRICT()
                .validate()
                .build(endOfStreamReader);

        CSVReader.Builder.COMMA_SEPARATED_TRIM_WHITESPACE()
                .validate()
                .build(endOfStreamReader);

        CSVReader.Builder.TAB_SEPARATED()
                .validate()
                .build(endOfStreamReader);

        CSVReader.Builder.ASCII_SEPARATED()
                .validate()
                .build(endOfStreamReader);
    }
}

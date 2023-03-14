package net.sentientturtle.csv;

import java.io.IOException;

/**
 * Exception to signal CSV parsing error
 */
public class CSVParseException extends Exception {
    // This exception always has to include a message detailing which parsing error occurred.
    // Message-less constructors are intentionally absent.
    CSVParseException(String message) {
        super(message);
    }
}

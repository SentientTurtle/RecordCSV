package net.sentientturtle.csv.exception;

/**
 * Exception to indicate boolean parsing failure
 */
public class BooleanParseException extends IllegalArgumentException {
    public BooleanParseException(String message) {
        super(message);
    }
}

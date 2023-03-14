package net.sentientturtle.csv.exception;

/**
 * Exception to indicate character parsing failure
 */
public class CharParseException extends Exception {
    public CharParseException(String message) {
        super(message);
    }
}

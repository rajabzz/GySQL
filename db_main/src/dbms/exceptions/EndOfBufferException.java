package dbms.exceptions;

/**
 * Created by blackvvine on 10/13/15.
 */
public class EndOfBufferException extends CoSQLQueryParseError {
    public EndOfBufferException(String message) {
        super(message);
    }

    public EndOfBufferException() {
    }
}

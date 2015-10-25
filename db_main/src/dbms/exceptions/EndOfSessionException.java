package dbms.exceptions;

/**
 * Created by blackvvine on 10/25/15.
 */
public class EndOfSessionException extends Exception {

    public EndOfSessionException() {
    }

    public EndOfSessionException(String message) {
        super(message);
    }
}

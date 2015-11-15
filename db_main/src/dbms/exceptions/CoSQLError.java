package dbms.exceptions;

/**
 * Created by blackvvine on 10/13/15.
 */
public class CoSQLError extends Exception {
    public CoSQLError() {
    }

    public CoSQLError(String message) {
        super(message);
    }

    public CoSQLError(String format, String... args) {
        super(String.format(format, args));
    }
}

package dbms.exceptions;

/**
 * Created by blackvvine on 10/13/15.
 */
public class CoSQLQueryParseError extends CoSQLError {
    public CoSQLQueryParseError() {
    }

    public CoSQLQueryParseError(String message) {
        super(message);
    }
}

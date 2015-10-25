package dbms.exceptions;

/**
 * Created by blackvvine on 10/13/15.
 */
public class CoSQLQueryExecutionError extends CoSQLError {
    public CoSQLQueryExecutionError() {
    }

    public CoSQLQueryExecutionError(String message) {
        super(message);
    }
}

package dbms.parser;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by blackvvine on 10/13/15.
 */
public abstract class CoSQLCommand {
    public abstract void execute() throws CoSQLQueryExecutionError, CoSQLError;
}

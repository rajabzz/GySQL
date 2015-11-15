package dbms.parser;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLCreateIndex extends CoSQLCommand {
    @Override
    public void execute() throws CoSQLQueryExecutionError, CoSQLError {
        System.out.println("INDEX CREATED");
    }
}

package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by blackvvine on 10/13/15.
 */
public class CoSQLCreateDatabase extends CoSQLCommand {
    String databaseName;

    public CoSQLCreateDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        DatabaseCore.createDatabase(databaseName);
    }
}

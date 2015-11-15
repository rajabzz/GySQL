package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by rajabzz on 10/29/15.
 */
public class CoSQLPrintTable extends CoSQLCommand {

    String tableName;

    public CoSQLPrintTable(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        DatabaseCore.printTable(tableName);
    }
}

package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryExecutionError;

import java.util.ArrayList;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLDelete extends CoSQLCommand {

    String tableName;
    String condition;

    public CoSQLDelete(String tableName, String condition) {
        this.tableName = tableName;
        this.condition = condition;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        DatabaseCore.delete(tableName, condition);
    }
}

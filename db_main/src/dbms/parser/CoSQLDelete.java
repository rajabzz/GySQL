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
    ArrayList<Table.Row> contentsMustBeDelete;

    public CoSQLDelete(String tableName, ArrayList<Table.Row> contentsMustBeDelete) {
        this.tableName = tableName;
        this.contentsMustBeDelete = contentsMustBeDelete;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        DatabaseCore.delete(tableName, contentsMustBeDelete);
    }
}

package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLError;

import java.util.ArrayList;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLUpdate extends CoSQLCommand {
    String tableName;
    String colName;
    String rawComputeValue;
    ArrayList<Table.Row> contents;

    public CoSQLUpdate(String tableName, String colName, String rawComputeValue, ArrayList<Table.Row> contents) {
        this.tableName = tableName;
        this.colName = colName;
        this.rawComputeValue = rawComputeValue;
        this.contents = contents;
    }

    @Override
    public void execute() throws CoSQLError {
        DatabaseCore.update(tableName, colName, rawComputeValue, contents);
    }
}

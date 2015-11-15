package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLError;

import java.util.ArrayList;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLSelect extends CoSQLCommand{

    String tableName;
    ArrayList<String> colNames;
    ArrayList<Table.Row> contents;

    public CoSQLSelect(String tableName, ArrayList<String> colNames, ArrayList<Table.Row> contents) {
        this.tableName = tableName;
        this.colNames = colNames;
        this.contents = contents;
    }

    @Override
    public void execute() throws CoSQLError {
        DatabaseCore.select(tableName, colNames, contents);
    }
}

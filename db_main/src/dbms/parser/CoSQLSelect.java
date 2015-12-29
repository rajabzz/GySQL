package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLError;

import java.util.ArrayList;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLSelect extends CoSQLCommand{

    ArrayList<String> tableNames;
    ArrayList<String> colNames;
    String rawTupleCondition;
    int type;

    public CoSQLSelect(ArrayList<String> tableName, ArrayList<String> colNames, String rawTupleCondition, int type) {
        this.tableNames = tableName;
        this.colNames = colNames;
        this.rawTupleCondition = rawTupleCondition;
        this.type = type;
    }

    @Override
    public void execute() throws CoSQLError {
        DatabaseCore.select(tableNames, colNames, rawTupleCondition, type);
    }
}

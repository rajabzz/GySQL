package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLError;

import java.util.ArrayList;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLSelect extends CoSQLCommand{

    String tableName;
    ArrayList<String> colNames;
    TupleCondition tupleCondition;

    public CoSQLSelect(String tableName, ArrayList<String> colNames, TupleCondition tupleCondition) {
        this.tableName = tableName;
        this.colNames = colNames;
        this.tupleCondition = tupleCondition;
    }

    @Override
    public void execute() throws CoSQLError {
        DatabaseCore.select(tableName, colNames, tupleCondition);
    }
}

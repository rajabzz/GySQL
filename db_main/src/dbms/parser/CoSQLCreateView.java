package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;

import java.util.ArrayList;

/**
 * Created by negin on 2/2/16.
 */
public class CoSQLCreateView extends CoSQLCommand {

    ArrayList<String> tableNames;
    ArrayList<SelectValue> selectValues;
    String rawTupleCondition;
    int type;
    GroupByData groupBy;
    String name;

    public CoSQLCreateView(String name ,ArrayList<String> tableNames, ArrayList<SelectValue> selectValues, String rawTupleCondition,
                           int type, GroupByData groupBy) {
        this.name = name;
        this.tableNames = tableNames;
        this.selectValues = selectValues;
        this.rawTupleCondition = rawTupleCondition;
        this.type = type;
        this.groupBy = groupBy;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError, CoSQLError {
        DatabaseCore.createView(name , tableNames , selectValues , rawTupleCondition , type  , groupBy);
    }
}

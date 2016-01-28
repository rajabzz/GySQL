package dbms.parser;

import dbms.engine.Table;

/**
 * Created by blackvvine on 1/28/16.
 */
public class SelectValue {

    enum Type {
        COLUMN_NAME, AGGREGATE_FUNCTION
    }

    Type type;

    String targetColumn;

    GroupByData.Method aggregateMethod;

    public static SelectValue forIndividualColumn(String columnName) {
        SelectValue res = new SelectValue();
        res.type = Type.COLUMN_NAME;
        res.targetColumn = columnName;
        return res;
    }

    public static SelectValue forGroupFunction(GroupByData.Method method, String colName) {
        SelectValue res = new SelectValue();
        res.type = Type.COLUMN_NAME;
        res.targetColumn = colName;
        res.aggregateMethod = method;
        return res;
    }

}

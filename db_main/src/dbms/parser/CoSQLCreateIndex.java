package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by rajabzz on 11/13/15.
 */
public class CoSQLCreateIndex extends CoSQLCommand {

    String indexName;
    String tableName;
    String columnName;

    public CoSQLCreateIndex(String indexName, String tableName, String columnName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError, CoSQLError {
        DatabaseCore.createIndex(indexName, tableName, columnName);
    }
}

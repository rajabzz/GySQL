package dbms.parser;

import java.util.List;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by blackvvine on 10/14/15.
 */
public class CoSQLCreateTable extends CoSQLCommand {

    String name;
    String type;

    List<Table.Column> columns;

    public CoSQLCreateTable(String name, List<Table.Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        DatabaseCore.createTable(name, columns);
    }
}

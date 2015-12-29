package dbms.parser;

import java.util.ArrayList;
import java.util.List;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Created by blackvvine on 10/14/15.
 */
public class CoSQLCreateTable extends CoSQLCommand {

    String name;
    String type;
    String PK ;
    ArrayList<String[]> FKarrays;

    List<Table.Column> columns;

    public CoSQLCreateTable(String name, List<Table.Column> columns, String PK , ArrayList<String[]> FKarrays) {
        this.name = name;
        this.columns = columns;
        this.PK = PK;
        this.FKarrays = FKarrays;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError , CoSQLError {
        DatabaseCore.createTable(name, columns , PK , FKarrays);
    }
}

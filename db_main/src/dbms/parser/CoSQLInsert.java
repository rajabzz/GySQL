package dbms.parser;

import java.util.ArrayList;

import dbms.exceptions.CoSQLQueryExecutionError;

/**
 * Author: Iman Akbari
 */
public class CoSQLInsert extends CoSQLCommand {

    String table;
    ArrayList<LexicalToken> values;

    public CoSQLInsert(String table, ArrayList<LexicalToken> values) {
        this.table = table;
        this.values = values;
    }

    @Override
    public void execute() throws CoSQLQueryExecutionError {
        // TODO fill
    }
}

package dbms.engine;

import com.sun.rowset.internal.Row;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.parser.LexicalToken;
import dbms.parser.QueryParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static dbms.util.LanguageUtils.throwExecError;

public class DatabaseCore {

    public static HashMap<String, Database> databases;

    public static Database defaultDatabase;
    public static Database currentDatabase;

    static void init() {

        // create default database
        defaultDatabase = new Database("DEFAULT");

        // create database list (as hash map, RBT really)
        databases = new HashMap<>();

        // add default database to DB list
        databases.put(defaultDatabase.name, defaultDatabase);

        // use default database as default
        currentDatabase = defaultDatabase;
    }

    static {
        init();
    }

    public static void createDatabase(String name) throws CoSQLQueryExecutionError {

        if (databases.containsKey(name)) {
            String error = String.format("Database with name \'%s\' already exists.", name);
            throw new CoSQLQueryExecutionError(error);
        }

        databases.put(name, new Database(name));

        System.out.println("DATABASE CREATED");
    }

    // TODO @Urgent null support
    public static void insert(String tableName, List<LexicalToken> values) throws CoSQLQueryExecutionError {

        Table target = currentDatabase.getTable(tableName);

        // check table exists
        if (target == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        // check argument count
        if (values.size() != target.getColumnCount()) {
            throwExecError("Given values and columns count not matching");
        }

        // check argument types
        for (int i=0; i<target.getColumnCount(); i++) {

            if (target.getColumnAt(i).type == Table.ColumnType.INT) {

                // expecting numeral value
                if (!values.get(i).getValue().matches(QueryParser.REGEX_NUMERAL)) {
                    throwExecError("Insert argument at index %d should be numeral (%s given)",
                            i, values.get(i).getValue()
                    );
                }

            } else if (target.getColumnAt(i).type == Table.ColumnType.VARCHAR) {

                // expecting literal value or null
                if (!values.get(i).isLiteral() && !values.get(i).getValue().equalsIgnoreCase("null")) {
                    throwExecError("Insert argument at index %d should be string literal (%s given)",
                        i, values.get(i).getValue()
                    );
                }

            } else {

                // avoiding later stupid bugs
                throw new IllegalStateException();

            }
        }

        // construct value set and parse data (convert to appropriate type)

        ArrayList<Object> dataValueSet = new ArrayList<>();

        for (int i=0; i<target.getColumnCount(); i++) {

            if (target.getColumnAt(i).type == Table.ColumnType.INT) { // if int

                // parse as number
                long parsed = Long.parseLong(values.get(i).getValue());
                dataValueSet.add(parsed);

            } else if (target.getColumnAt(i).type == Table.ColumnType.VARCHAR) { // if varchar

                if (values.get(i).getValue().equalsIgnoreCase("null")) {
                    // add directly
                    dataValueSet.add(null);
                } else {
                    dataValueSet.add(values.get(i).getValue());
                }

            }

        }

        // finally, insert the parsed
        target.insertRow(dataValueSet);
        System.out.println("RECORD INSERTED");
    }

    public static void createTable(String name, List<Table.Column> columns) throws CoSQLQueryExecutionError {

        // TODO check names legal

        // check if name is not unique
        for (Table table: currentDatabase.tables.values()) {
            if (table.tableName.equals(name)) {
                String error = String.format("Database \'%s\' already contains a table by the name: \'%s\'",
                        currentDatabase.name,
                        name
                );
                throw new CoSQLQueryExecutionError(error);
            }
        }

        // instantiate
        Table newTable = new Table(name);

        // create columns
        for (Table.Column c: columns) {

            newTable.addColumn(c);

        }

        // add the new table
        currentDatabase.addTable(newTable);

        // user feedback
        String message = "TABLE CREATED";
        System.out.println(message);
    }


    public static void printTable(String tableName) throws CoSQLQueryExecutionError {
        Table table = defaultDatabase.getTable(tableName);

        // if table doesn't exist
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        System.out.println(table);
    }
}

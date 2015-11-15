package dbms.engine;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.parser.ComputeValue;
import dbms.parser.LexicalToken;
import dbms.parser.QueryParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static dbms.util.LanguageUtils.throwExecError;

public class DatabaseCore {
    public static final int COMPARISON_TYPE_EQUAL = 0;
    public static final int COMPARISON_TYPE_GREATER = 1;
    public static final int COMPARISON_TYPE_GREATER_OR_EQUAL = 2;
    public static final int COMPARISON_TYPE_LESS_THAN = 3;
    public static final int COMPARISON_TYPE_LESS_THAN_OR_EQUAL = 4;

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


    public static void update (String tableName, String colName, String rawComputeValue, ArrayList<Table.Row> contents) throws CoSQLError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        ArrayList<Integer> contentIndexes = new ArrayList<>();
        int colIndex = table.getColumnIndex(colName);
        for (Table.Row row: contents) {
            contentIndexes.add(table.getRowIndex(row));
        }

        for (Integer index: contentIndexes) {
            LexicalToken computeValue = ComputeValue.compute(rawComputeValue, table, index);
            if (computeValue.isLiteral()) {
                table.getRowAt(index).updateValueAt(colIndex, computeValue.getValue());
            } else {
                table.getRowAt(index).updateValueAt(colIndex, Long.parseLong(computeValue.getValue()));
            }
        }
    }


    public static void delete(String tableName, ArrayList<Table.Row> contentsMustBeDelete) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        for (Table.Row row: contentsMustBeDelete) {
            table.removeRow(row);
        }
    }

    public static void select(String tableName, ArrayList<String> colNames, ArrayList<Table.Row> contents) throws CoSQLError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }
        ArrayList<Integer> colIndexes = new ArrayList<>();
        ArrayList<Table.Column> columns = new ArrayList<>();
        for (String colName: colNames) {
            colIndexes.add(table.getColumnIndex(colName));
            columns.add(table.getColumn(colName));
        }
        ArrayList<Table.Row> finalContents = new ArrayList<>();
        for (Table.Row row: contents) {
            ArrayList<Object> values = new ArrayList<>();
            for (Integer i: colIndexes) {
                values.add(row.getValueAt(i));
            }
            finalContents.add(new Table.Row(values));
        }

        Table finalTable = new Table("printable", columns, finalContents);
        System.out.println(finalTable);
    }

    public static ArrayList<Table.Row> getContents(String tableName, String colName, String computeValueQuery, int type) throws CoSQLQueryExecutionError, CoSQLQueryParseError {
        Table table = currentDatabase.getTable(tableName);

        // check table exists
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        ArrayList<Table.Row> resultRows = new ArrayList<>();
        int colIndex = -1;
        for (int i = 0; i < table.getColumnCount(); i++) {
            if (table.getColumnAt(i).name.equalsIgnoreCase(colName)) {
                colIndex = i;
                break;
            }
        }

        if (colIndex == -1)
            throwExecError("The given column name doesn't match to any case: %s", colName);

        Table.ColumnType colType = table.getColumnAt(colIndex).type;


        for (int i = 0; i < table.getRowCount(); i++) {

            LexicalToken computedValue = ComputeValue.compute(computeValueQuery, table, i);
            String value = computedValue.getValue();
            Table.Row row = table.getRowAt(i);
            Object objVal = row.getValueAt(colIndex); // TODO may cause some bugs !

            switch (type) {
                case COMPARISON_TYPE_EQUAL:
                        if (colType.equals(Table.ColumnType.INT)) {
                            if (Long.parseLong(value) == (Long) objVal)
                                resultRows.add(row);
                        } else {
                            if (value.equals(objVal))
                                resultRows.add(row);
                        }
                    break;

                case COMPARISON_TYPE_GREATER:
                        if (colType.equals(Table.ColumnType.INT)) {
                            if (Long.parseLong(value) > (Long) objVal)
                                resultRows.add(row);
                        } else {
                            // string comparison ...
                        }

                    break;

                case COMPARISON_TYPE_GREATER_OR_EQUAL:

                        if (colType.equals(Table.ColumnType.INT)) {
                            if (Long.parseLong(value) >= (Long) objVal)
                                resultRows.add(row);
                        } else {
                            // string comparison ...
                        }

                    break;

                case COMPARISON_TYPE_LESS_THAN:
                        if (colType.equals(Table.ColumnType.INT)) {
                            if (Long.parseLong(value) < (Long) objVal)
                                resultRows.add(row);
                        } else {
                            // string comparison ...
                        }

                    break;

                case COMPARISON_TYPE_LESS_THAN_OR_EQUAL:

                        if (colType.equals(Table.ColumnType.INT)) {
                            if (Long.parseLong(value) <= (Long) objVal)
                                resultRows.add(row);
                        } else {
                            // string comparison ...
                        }

                    break;

                default:
                    System.err.println("mage msihe ?! :|");
                    break;
            }
        }
        return resultRows;
    }

    public static void printTable(String tableName) throws CoSQLQueryExecutionError {
        Table table = defaultDatabase.getTable(tableName);

        // if table doesn't exist
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        System.out.println(table);
    }

    public static Table getTable (String tableName) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);
        // if table doesn't exist
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }
        return table;
    }
}

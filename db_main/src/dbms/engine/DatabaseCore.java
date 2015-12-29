package dbms.engine;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.parser.ComputeValue;
import dbms.parser.LexicalToken;
import dbms.parser.QueryParser;
import dbms.parser.TupleCondition;
import dbms.parser.ValueComputer;

import javax.management.Query;
import java.util.*;

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

                LexicalToken tk = values.get(i);

                if (tk.getValue().equalsIgnoreCase("null") && !tk.isLiteral()) {
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

    public static void createIndex(String indexName, String tableName, String columnName) throws CoSQLQueryExecutionError {

        Table table = currentDatabase.tables.get(tableName);

        if (table == null) {
            String s = String.format("No such table: '%s' exists in database.", tableName);
            throw new CoSQLQueryExecutionError(s);
        }

        Table.Column column = null;
        try {
            column = table.getColumn(columnName);
        } catch (CoSQLError coSQLError) {
            String err = String.format("No such column: '%s' exists in table %s.",
                    columnName,
                    tableName
            );
            throw new CoSQLQueryExecutionError(err);
        }

        Table.Index index = table.indexes.get(column);
        if (index != null) {
            throw new CoSQLQueryExecutionError("An index called '%s' already exists on column %s",
                    index.name,
                    column.getName()
            );
        }

        // instantiate new index and add to table
        index = new Table.Index(indexName, column);
        table.addIndex(index);

        // create index and add rows
        for (Table.Row row: table.getRows()) {
            table.indexRow(row, index);
        }

        String message = "INDEX CREATED";
        System.out.println(message);

    }


    public static void update (String tableName, String colName, String rawComputeValue, ArrayList<Table.Row> contents) throws CoSQLError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        /*ArrayList<Integer> contentIndexes = new ArrayList<>();
        int colIndex = table.getColumnIndex(colName);
        for (Table.Row row: contents) {
            contentIndexes.add(table.getRowIndex(row));
        }*/

        int colIndex = table.getColumnIndex(colName);

        ValueComputer.ParsedTuple tuple = ValueComputer.computeFieldBased(rawComputeValue, table);

        for (Table.Row row: contents) {
            //LexicalToken computeValue = ComputeValue.compute(rawComputeValue, table, index);

            Object computeValue = tuple.computeForRow(row);
            row.updateValueAt(colIndex, computeValue);

            /*if (computeValue.isLiteral()) {
                table.getRowAt(index).updateValueAt(colIndex, computeValue.getValue());
                table.updateIndexAt(colIndex, index);
            } else {
                table.getRowAt(index).updateValueAt(colIndex, Long.parseLong(computeValue.getValue()));
                table.updateIndexAt(colIndex, index);
            }*/
        }
    }


    public static void delete(String tableName, ArrayList<Table.Row> contentsMustBeDelete) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        for (Table.Row row: new ArrayList<Table.Row>(contentsMustBeDelete)) {
            table.removeRow(row);
            table.updateIndexForDelete(row);
        }
    }

    public static void select(String tableName, ArrayList<String> colNames, String rawTupleCondition) throws CoSQLError {
        // checking if table exists
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        select(table, colNames, rawTupleCondition);
    }

    public static void select(Table table, ArrayList<String> colNames, String rawTupleCondition) throws CoSQLError {
        // get contents of tuple condition
        TupleCondition tupleCondition = new TupleCondition(rawTupleCondition, table.tableName);
        List<Table.Row> contents = tupleCondition.getContents();

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


    public static void select(ArrayList<String> tableNames, ArrayList<String> colNames, String rawTupleCondition, int type) throws CoSQLError {
        Table table1 = currentDatabase.getTable(tableNames.get(0));
        Table table2 = currentDatabase.getTable(tableNames.get(1));
        if (table1 == null || table2 == null) {
            throwExecError("Incorrect table name(s) !");
        }
        Table resultTable = (type == QueryParser.JOIN ? table1.join(table2) : table1.cartesianProduct(table2));

        select(resultTable, colNames, rawTupleCondition);
    }

    private static boolean objectEquals(Object o1, Object o2) {

        if (o1 == o2) {
            return true;
        }

        if (o1 == null) {
            return false;
        }

        return o1.equals(o2);
    }

    public static ArrayList<Table.Row> getContents(String tableName, String colName, String computeValueQuery, int type) throws CoSQLQueryExecutionError, CoSQLQueryParseError {

        Table table = currentDatabase.getTable(tableName);

        // check table exists
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        // get column index
        ArrayList<Table.Row> resultRows = new ArrayList<>();
        int colIndex = -1;
        for (int i = 0; i < table.getColumnCount(); i++) {
            if (table.getColumnAt(i).name.equalsIgnoreCase(colName)) {
                colIndex = i;
                break;
            }
        }

        // throw if not found
        if (colIndex == -1)
            throwExecError("The given column name doesn't match < any case: %s", colName);

        // fetch column and type
        Table.ColumnType colType = table.getColumnAt(colIndex).type;
        Table.Column column = table.getColumnAt(colIndex);

        // fetch index if any
        Table.Index idx = table.indexes.get(column);

        // parse query for its type
        ValueComputer.ValueType valueType = ValueComputer.getType(computeValueQuery);

        // compute right hand side of operator if it's constant,
        // otherwise defer an object which computes value for row
        Object constantValue = null;
        ValueComputer.ParsedTuple computer = null;
        if (valueType == ValueComputer.ValueType.CONSTANT) {
            constantValue = ValueComputer.computeConstant(computeValueQuery);
        } else {
            computer = ValueComputer.computeFieldBased(computeValueQuery, table);
        }

        // if no index on selected column, or query is field based
        if (idx == null || valueType == ValueComputer.ValueType.FIELD_BASED) {

            // iterate through table rows
            for (int i = 0; i < table.getRowCount(); i++) {

                // fetch row
                Table.Row row = table.getRowAt(i);

                // decide compute value for row or use the precomputed constant value
                Object computedValue =
                        valueType == ValueComputer.ValueType.FIELD_BASED ?
                                computer.computeForRow(row) : constantValue;

                Object rowValue = row.getValueAt(colIndex);

                if (type == COMPARISON_TYPE_EQUAL) { /* check equality */

                    // check equality and add if positive
                    if (objectEquals(rowValue, computedValue)) {
                        resultRows.add(row);
                    }

                } else if (rowValue instanceof Comparable) { /* check comparison */

                    // compare values
                    int cmp = ((Comparable)rowValue).compareTo(computedValue);

                    if ((type == COMPARISON_TYPE_GREATER && cmp > 0) ||
                            (type == COMPARISON_TYPE_GREATER_OR_EQUAL && cmp >= 0) ||
                            (type == COMPARISON_TYPE_LESS_THAN && cmp < 0) ||
                            (type == COMPARISON_TYPE_LESS_THAN_OR_EQUAL && cmp <= 0)) {

                        resultRows.add(row);
                    }

                }

                // TODO throw error for non-comparables somewhere before

            } // end for loop

        } else { /* using index */

            switch (type) {

                case COMPARISON_TYPE_EQUAL: {

                    // check index values for given constant and
                    // return data if any
                    HashSet<Table.Row> indexedResult = idx.index.get(constantValue);
                    if (indexedResult != null) {
                        resultRows.addAll(indexedResult);
                    }

                    break;
                }

                case COMPARISON_TYPE_GREATER: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(constantValue, false, idx.index.lastKey(), true).values();

                    for (HashSet<Table.Row> s: sets) {
                        resultRows.addAll(s);
                    }

                    break;
                }

                case COMPARISON_TYPE_GREATER_OR_EQUAL: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(constantValue, true, idx.index.lastKey(), true).values();

                    for (HashSet<Table.Row> s: sets) {
                        resultRows.addAll(s);
                    }

                    break;
                }

                case COMPARISON_TYPE_LESS_THAN: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(idx.index.firstKey(), true, computeValueQuery, false).values();

                    for (HashSet<Table.Row> s: sets) {
                        for (Table.Row r: s) {
                            if (r.getValueAt(colIndex) != null)
                                resultRows.add(r);
                        }
                    }

                    break;
                }

                case COMPARISON_TYPE_LESS_THAN_OR_EQUAL: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(idx.index.firstKey(), true, computeValueQuery, true).values();

                    for (HashSet<Table.Row> s: sets) {
                        for (Table.Row r: s) {
                            if (r.getValueAt(colIndex) != null)
                                resultRows.add(r);
                        }
                    }

                    break;
                }

                default:
                    System.err.println("mage msihe ?! :|");
                    break;

            }

        } // end index use

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

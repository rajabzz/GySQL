package dbms.engine;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.parser.GroupByData;
import dbms.parser.LexicalToken;
import dbms.parser.QueryParser;
import dbms.parser.TupleCondition;
import dbms.parser.ValueComputer;
import dbms.util.GroupHashMap;

import java.util.*;

import static dbms.util.LanguageUtils.throwExecError;

import static dbms.engine.Table.Row;
import static dbms.engine.Table.Column;

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
        for (int i = 0; i < target.getColumnCount(); i++) {

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

        for (int i = 0; i < target.getColumnCount(); i++) {

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

        //check for C1 when inserting  pk column
        boolean isPKError = false;
        // if(!target.getPKcolumn().equals(null)) {
        int pkIndex = target.getColumnIndex(target.getPKcolumn());
        if (pkIndex != -1) {
            for (Table.Row prev : target.getContents()) {
                if (prev.getValueAt(pkIndex).equals(dataValueSet.get(pkIndex)) || dataValueSet.get(pkIndex).equals(null)) {
                    System.out.println("C1 CONSTRAINT FAILED");
                    isPKError = true;
                    break;
                }
            }
        }
        // }

        //C2 check for inserting data in fk columns
        Table reference;
        int j = -1;
        boolean interrupt = false;
        for (Table.Column c : target.FKcolumns) {
            j++;
            int fkIndex = target.getColumnIndex(c);
            // for (int i = 0; i < dataValueSet.size(); i++) {
            //   if (dataValueSet.get(i).equals(target.getColumnIndex(fkIndex))) {
            reference = target.tableReference.get(j);
            int pkIndexRef = reference.getColumnIndex(reference.getPKcolumn());
            Object value = dataValueSet.get(fkIndex);
            if (!reference.exists(value, pkIndexRef)) {
                interrupt = true;
                System.out.println("C2 CONSTRAINT FAILED");
                break;
            }
//            for (Table.Row prev : reference.contents) {
//                System.out.println("data is : " + dataValueSet.get(fkIndex));
//                System.out.println("prev is : " + prev.getValueAt(pkIndexRef));
//                if (prev.getValueAt(pkIndexRef).equals(dataValueSet.get(fkIndex))) {
//                    break;
//                    interrupt = true;
//                    System.out.println("i is : " + interrupt);
//                }
//            }


        }

        if (interrupt == false && isPKError == false) {
            // finally, insert the parsed
            target.insertRow(dataValueSet);
            System.out.println("RECORD INSERTED");
        }
//        } else {
//            target.insertRow(dataValueSet);
//
//            interrupt = false;
//            System.out.println("RECORD INSERTED");
//        }

        //  System.out.println(target);
    }

    public static void createTable(String name, List<Table.Column> columns, String PK, ArrayList<String[]> FKDetails) throws CoSQLQueryExecutionError, CoSQLError {


        // TODO check names legal

        // check if name is not unique
        for (Table table : currentDatabase.tables.values()) {
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
        Table target; //reference table
//        ArrayList<Table.Column> Fkcolumn = newTable.FKcolumns;
//        ArrayList<Table> fkTableReference = newTable.tableReference;

        // create columns
        for (Table.Column c : columns) {

            newTable.addColumn(c);

        }

        if (PK != null)
            newTable.setPKcolumn(PK);
        for (String[] s : FKDetails) {
            String FK = s[0];

            target = currentDatabase.getTable(s[1]);
            target.listener.add(newTable); //pk has references

            if (!target.getPKcolumn().equals(null)) {
                newTable.FKcolumns.add(newTable.setFKcolumn(FK));
                newTable.tableReference.add(target); //Fk is referenced to
            } else
                System.out.println("C2 CONSTRAINT FAILED");

            String delete = s[2];
            String update = s[3];
            newTable.onUpdate.add(update);
            newTable.onDelete.add(delete);
        }

        // add the new table
        currentDatabase.addTable(newTable);
        if (newTable.getPKcolumn() != null)
            initialCreateIndex("PKIndex", newTable, newTable.getPKcolumn());
        for (Table.Column fkCol: newTable.FKcolumns)
            initialCreateIndex("FKIndex: " + fkCol.getName(), newTable, fkCol);

        // user feedback
        String message = "TABLE CREATED";
        System.out.println(message);
    }

    private static void initialCreateIndex(String indexName, Table table, Table.Column column) {

        // instantiate new index and add to table
        Table.Index index = new Table.Index(indexName, column);
        table.addIndex(index);

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

        if ((table.getPKcolumn() != null && table.getPKcolumn().equals(column)) || table.FKcolumns.contains(column)) {
            table.indexes.get(column).name = indexName;
            String message = "INDEX CREATED";
            System.out.println(message);
            return;
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
        for (Table.Row row : table.getRows()) {
            table.indexRow(row, index);
        }

        String message = "INDEX CREATED";
        System.out.println(message);

    }

    public static void update(String tableName, String colName, String rawComputeValue, ArrayList<Table.Row> contents) throws CoSQLError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        /*ArrayList<Integer> contentIndexes = new ArrayList<>();
        int colIndex = table.getColumnIndex(colName);
        for (Table.Row row: contents) {
            contentIndexes.add(table.getRowIndex(row));
        }*/

        //checking if the wanted column is Fk and get its table reference
        boolean isFK = false;
        Table.Column FKwanted;
        Table refTable = null;
        int pkColIndex = -1;
        int i = -1;
        for (Table.Column c : table.FKcolumns) {
            i++;
            if (c.getName().equals(colName)) {
                FKwanted = c;
                isFK = true;
                refTable = table.tableReference.get(i);
                pkColIndex = refTable.getColumnIndex(refTable.getPKcolumn());
            }
        }

        int colIndex = table.getColumnIndex(colName);

        ValueComputer.ParsedTuple tuple = ValueComputer.computeFieldBased(rawComputeValue, table);
        int col;
        boolean error = false;

        for (Table.Row row : contents) {
            //LexicalToken computeValue = ComputeValue.compute(rawComputeValue, table, index);

            Object computeValue = tuple.computeForRow(row);
            if (colIndex == table.getColumnIndex(table.getPKcolumn())) {
                for (Table.Row r : table.getContents()) {
                    if (computeValue.equals(r.getValueAt(colIndex))) {
                        System.out.println("C1 CONSTRAINT FAILED");
                        error = true;
                        break;

                    }

                }
                if (error) {
                    continue;
                }
                int cool = -1;
                //TODO index e row pk vojud nadare
                Object obj = row.getValueAt(table.getColumnIndex(table.getPKcolumn()));
                for (Table l : table.listener) {
                    for (int z = 0; z < l.tableReference.size(); z++) {
                        if (table.equals(l.tableReference.get(z))) {
                            cool = l.getColumnIndex(l.FKcolumns.get(z));

//                            for (int k = 0; k < l.tableReference.size(); k++) {
//                                if (table.equals(l.tableReference.get(k))) {
                            if (l.onUpdate.get(z).equalsIgnoreCase("restrict") && l.exists(obj, cool)) {
                                error = true;
                                System.out.println("FOREIGN KEY CONSTRAINT RESTRICTS");
                                break;
                            }
//                                }
//                                if (error == true)
//                                    break;
//                            }
                        }
                    }
                    if (error) {
                        continue;
                    }
                    row.updateValueAt(colIndex, computeValue); //pk updates
                    // fk updates
                    for (Table target : table.listener) {
                        for (int j = 0; j < target.FKcolumns.size(); j++) {
                            if (target.tableReference.get(j).equals(table)) {
                                col = target.getColumnIndex(target.FKcolumns.get(j));
                                for (Table.Row r2 : target.getContents()) {
                                    if (r2.getValueAt(col).equals(obj))
                                        r2.updateValueAt(col, computeValue);
                                }
                            }
                        }
                    }
                }
            } else if (isFK) {

                if (!refTable.exists(computeValue, pkColIndex)) {
                    System.out.println("C2 CONSTRAINT FAILED");
                    continue;
                } else
                    row.updateValueAt(colIndex, computeValue);
            } else
                row.updateValueAt(colIndex, computeValue);


            /*if (computeValue.isLiteral()) {
                table.getRowAt(index).updateValueAt(colIndex, computeValue.getValue());
                table.updateIndexAt(colIndex, index);
            } else {
                table.getRowAt(index).updateValueAt(colIndex, Long.parseLong(computeValue.getValue()));
                table.updateIndexAt(colIndex, index);
            }*/


        }

        //  System.out.println(table);
    }


//    public static void delete(String tableName, ArrayList<Table.Row> contentsMustBeDelete) throws CoSQLQueryExecutionError {
//        Table table = currentDatabase.getTable(tableName);
//
//        if (table == null) {
//            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
//        }
//
//        // Object obj = null;
//        for (Table.Row row : new ArrayList<Table.Row>(contentsMustBeDelete)) {
//
//            //if pk doesn't have references then delete it
//            if (table.listener.isEmpty()) {
//                table.removeRow(row);
//                table.updateIndexForDelete(row);
//            } else {
//
////                for (Table.Row r : table.getContents()){
////                    if(row.)
////                     obj = r.getValueAt(table.getColumnIndex(table.getPKcolumn()));
////                }
//
//                Object obj = row.getValueAt(table.getColumnIndex(table.getPKcolumn()));
//                int cool = -1;
//                for (Table l : table.listener) {
//
//                    for (int i = 0; i < l.tableReference.size(); i++) {
//                        if (table.equals(l.tableReference.get(i))) {
//                            cool = l.getColumnIndex(l.FKcolumns.get(i));
//                        }
//                        if (cool != -1) {
//                            for (int k = 0; k < l.tableReference.size(); k++) {
//                                if (table.equals(l.tableReference.get(k))) {
//                                    if (l.onDelete.get(k).equalsIgnoreCase("restrict") && l.exists(obj, cool)) {
//                                        //referenced FK was restrict and the record existed
//                                        System.out.println("FOREIGN KEY CONSTRAINT RESTRICTS");
//                                        break;
//                                    } else if (l.onDelete.get(k).equalsIgnoreCase("cascade") && l.exists(obj, cool)) {
//
//                                        delete(l.getName(), contentsMustBeDelete);
//
//                                    } else if (l.onDelete.get(k).equalsIgnoreCase("restrict") && !l.exists(obj, cool)) {
//                                        //referenced FK was restrict but the record didn't exist
//                                        table.removeRow(row);
//                                        table.updateIndexForDelete(row);
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        //    System.out.println(table);
//        }
//    }

    static class TR {
        Table table;
        Table.Row row;

        TR(Table table, Table.Row row) {
            this.table = table;
            this.row = row;
        }
    }

    private static ArrayList<TR> deletedNodes = new ArrayList<>();

    private static void deleteARow(Table.Row r, Table t) {
        Object obj = r.getValueAt(t.getColumnIndex(t.getPKcolumn()));

        for (Table l : t.listener) {
            int i;
            for (i = 0; i < l.tableReference.size(); i++) {
                if (l.tableReference.get(i).equals(t)) {
                    break;
                }
            }

            int relatedFKIndex = l.getColumnIndex(l.FKcolumns.get(i));
            boolean relatedRestrict = (l.onDelete.get(i).equals("restrict")) ? true : false;

            for (Table.Row row : l.getContents()) {
                if (row.getValueAt(relatedFKIndex).equals(obj)) {
                    deleteARow(row, l);
                }
            }

        }
        deletedNodes.add(new TR(t, r));

//        if (t.listener.isEmpty()) {
//            deletedNodes.add(new TR(t, r));
//        } else {
//
//            Object obj = r.getValueAt(t.getColumnIndex(t.getPKcolumn()));
//            rest = false;
//            for (Table l : t.listener) {
//                int i;
//                for (i = 0; i < l.tableReference.size(); i++) {
//                    if (l.tableReference.get(i).equals(t)) {
//                        break;
//                    }
//                }
//
//                int relatedFKIndex = l.getColumnIndex(l.FKcolumns.get(i));
//                boolean relatedRestrict = (l.onDelete.get(i).equals("restrict")) ? true : false;
//
//                for (Table.Row row : l.getContents()) {
//                    if (row.getValueAt(relatedFKIndex).equals(obj)) {
//                        if (relatedRestrict) {
//                            rest = true;
//                            System.out.println(rest+l.getName());
//                            return;
//                        } else {
//                            deleteARow(row, l);
//                        }
//                    }
//
//                }
//            }
//            if (!rest)
//                deletedNodes.add(new TR(t, r));
//        }
    }

    private static boolean restrictDFS(Table.Row r, Table t) {
        if (t.listener.isEmpty()) {
            return false;
        } else {
            Object obj = r.getValueAt(t.getColumnIndex(t.getPKcolumn()));
            for (Table l : t.listener) {
                int i;
                for (i = 0; i < l.tableReference.size(); i++) {
                    if (l.tableReference.get(i).equals(t)) {
                        break;
                    }
                }

                int relatedFKIndex = l.getColumnIndex(l.FKcolumns.get(i));
                boolean relatedRestrict = (l.onDelete.get(i).equals("restrict")) ? true : false;

                for (Table.Row row : l.getContents()) {
                    if (row.getValueAt(relatedFKIndex).equals(obj)) {
                        if (relatedRestrict) {
                            return true;
                        } else if (restrictDFS(row, l)) {
                            return true;
                        }
                    }

                }
            }
            return false;
        }
    }

    public static void delete(String tableName, ArrayList<Table.Row> contentsMustBeDelete) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        for (Table.Row row : new ArrayList<Table.Row>(contentsMustBeDelete)) {
            if (!restrictDFS(row, table)) {
                deleteARow(row, table);
                for (TR d : deletedNodes) {
                    d.table.removeRow(d.row);
                    d.table.updateIndexForDelete(d.row);
                }
                deletedNodes.clear();
            } else {
                System.out.println("FOREIGN KEY CONSTRAINT RESTRICTS");
                break;
            }
        }
    }

    public static Table select(String tableName, ArrayList<String> colNames, String rawTupleCondition) throws CoSQLError {
        // checking if table exists
        Table table = currentDatabase.getTable(tableName);
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        return select(table, colNames, rawTupleCondition);
    }

    public static Table select(Table table, ArrayList<String> colNames, String rawTupleCondition) throws CoSQLError {

        // get contents of tuple condition
        TupleCondition tupleCondition = new TupleCondition(rawTupleCondition, table.tableName);
        List<Table.Row> contents = tupleCondition.getContents();

        ArrayList<Integer> colIndexes = new ArrayList<>();
        ArrayList<Table.Column> columns = new ArrayList<>();
        for (String colName : colNames) {
            colIndexes.add(table.getColumnIndex(colName));
            columns.add(table.getColumn(colName));
        }

        ArrayList<Table.Row> finalContents = new ArrayList<>();
        for (Table.Row row : contents) {
            ArrayList<Object> values = new ArrayList<>();
            for (Integer i : colIndexes) {
                values.add(row.getValueAt(i));
            }
            finalContents.add(new Table.Row(values));
        }

        return new Table("printable", columns, finalContents);

    }

    public static Table select(ArrayList<String> tableNames, ArrayList<String> colNames, String rawTupleCondition, int type, GroupByData groupBy) throws CoSQLError {

        Table table1 = currentDatabase.getTable(tableNames.get(0));
        if (tableNames.size() == 1) {
            return select(table1, colNames, rawTupleCondition);
        }

        Table table2 = currentDatabase.getTable(tableNames.get(1));
        Table resultTable;
        if (type == QueryParser.JOIN) {
            if (table1.tableReference.contains(table2))
                resultTable = table1.join(table2);
            else if (table2.tableReference.contains(table1))
                resultTable = table2.join(table1);
            else
                throw new CoSQLQueryExecutionError("Not such tables can be joint");

        } else
            resultTable = table1.cartesianProduct(table2);

        currentDatabase.addTable(resultTable);

        Table res = select(resultTable, colNames, rawTupleCondition);

        currentDatabase.removeTable(resultTable); // TODO move this to table destructor

        return res;

    }

    public static Table groupTable(Table table, GroupByData groupBy) throws CoSQLQueryExecutionError {

        // resolve columns
        List<Table.Column> groupColumns = new ArrayList<>();
        List<Integer> groupColumnsPositions = new ArrayList<>();
        for (String colName: groupBy.getColumns()) {
            try {
                Column col = table.getColumn(colName);
                groupColumns.add(col);
                groupColumnsPositions.add(table.getColumnIndex(col));
            } catch (CoSQLError coSQLError) {
                coSQLError.printStackTrace();
                throw new CoSQLQueryExecutionError("Cannot resolve column name: " + colName);
            }
        }

        Table res = new Table("GROUP_RESULT", table.columns, new ArrayList<Row>());

        GroupHashMap<List<Object>, Row> map = new GroupHashMap<>();

        // iterate table rows
        for (Row row: table.getRows()) {

            // make a tuple of group columns' values
            List<Object> valuesTuple = new ArrayList<>();
            for (Integer idx : groupColumnsPositions) {
                // iterates through the group columns and adds
                // their values in rows to the tuple
                valuesTuple.add(row.getValueAt(idx));
            }

            map.add(valuesTuple, row);

        }

        // TODO @Urgent fill

        return res;
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
                    int cmp = ((Comparable) rowValue).compareTo(computedValue);

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

                    for (HashSet<Table.Row> s : sets) {
                        resultRows.addAll(s);
                    }

                    break;
                }

                case COMPARISON_TYPE_GREATER_OR_EQUAL: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(constantValue, true, idx.index.lastKey(), true).values();

                    for (HashSet<Table.Row> s : sets) {
                        resultRows.addAll(s);
                    }

                    break;
                }

                case COMPARISON_TYPE_LESS_THAN: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(idx.index.firstKey(), true, computeValueQuery, false).values();

                    for (HashSet<Table.Row> s : sets) {
                        for (Table.Row r : s) {
                            if (r.getValueAt(colIndex) != null)
                                resultRows.add(r);
                        }
                    }

                    break;
                }

                case COMPARISON_TYPE_LESS_THAN_OR_EQUAL: {

                    Collection<HashSet<Table.Row>> sets = idx.index.subMap(idx.index.firstKey(), true, computeValueQuery, true).values();

                    for (HashSet<Table.Row> s : sets) {
                        for (Table.Row r : s) {
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

    public static Table getTable(String tableName) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);
        // if table doesn't exist
        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }
        return table;
    }
}

package dbms.engine;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.parser.*;
import dbms.util.GroupHashMap;

import java.util.*;

import static dbms.util.LanguageUtils.throwExecError;

import static dbms.engine.Table.Row;
import static dbms.engine.Table.Column;
import static dbms.parser.GroupByData.Method;

public class DatabaseCore {

    public static final int COMPARISON_TYPE_EQUAL = 0;
    public static final int COMPARISON_TYPE_GREATER = 1;
    public static final int COMPARISON_TYPE_GREATER_OR_EQUAL = 2;
    public static final int COMPARISON_TYPE_LESS_THAN = 3;
    public static final int COMPARISON_TYPE_LESS_THAN_OR_EQUAL = 4;

    public static HashMap<String, Database> databases;

    public static Database defaultDatabase;
    public static Database currentDatabase;

    public static Database getCurrentDatabase() {
        return currentDatabase;
    }

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

    public static void createView(String name, ArrayList<String> tableNames, ArrayList<SelectValue> selectValues,
                                  String rawTupleCondition, int type, GroupByData groupBy) throws CoSQLError {

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

        //making the view and adding to database

        Table tlb = select(tableNames, selectValues, rawTupleCondition, type, groupBy);
        View view = new View(tlb.getName(), tlb.getColumns(), tlb.getContents());
        view.setTableName(name);
        currentDatabase.addTable(view);

        Table t = null;
        /** searching for PK **/
        for (String Tname : tableNames) {
            t = currentDatabase.getTable(Tname);
            view.originalTables.add(t);
            for (Column c : view.getColumns()) {
                if (c.getName().equals(t.getPKcolumn().getName()))
                    view.setPKcolumn(c.getName());
            }
        }

        //if there is no Pk then its not pazira
        if (view.PKcolumns.isEmpty() || view.originalTables.size() > 1 || groupBy != null)
            view.isPazira = false;


        String message = "VIEW CREATED";
        System.out.println(message);
    }

    public static void insert(String tableName, List<LexicalToken> values) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        if (table instanceof View) {
            View v = (View) table;
            insertItemsForView(v, values, false);
        } else
            insertItems(table.getName(), values, false);

    }


    public static boolean insertItemsForView(View view, List<LexicalToken> values, boolean viewIsReference) throws CoSQLQueryExecutionError {
        if (!view.isPazira) {
            System.out.println(String.format("VIEW %s IS NOT UPDATABLE", view.getName()));
            return false;
        }

        //insert khodesh va reference hash

        boolean error = false;

        for (Table tbl : view.originalTables) {
            HashMap<Integer, LexicalToken> colValue = new HashMap<>();

            for (int i = 0; i < view.getColumnCount(); i++) {
                colValue.put(tbl.getColumnIndex(view.getColumnAt(i)), values.get(i));
            }

            List<LexicalToken> valuesMustBeInserted = new ArrayList<>();
            for (int i = 0; i < tbl.getColumnCount(); i++) {
                if (colValue.containsKey(i)) {
                    valuesMustBeInserted.add(colValue.get(i));
                } else {
                    valuesMustBeInserted.add(new LexicalToken("NULL", false));
                }
            }
            if (tbl instanceof View) {
                View v = (View) tbl;
                error = !insertItemsForView(v, valuesMustBeInserted, true);
            }
            else
                error = !insertItems(tbl.getName(), valuesMustBeInserted, true);
        }

        if (!error)
            return insertItems(view.getName(), values, viewIsReference);

        return false;
    }

    // TODO @Urgent null support
    public static boolean insertItems(String tableName, List<LexicalToken> values, boolean isReferenceTable) throws CoSQLQueryExecutionError {

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
                if (!values.get(i).getValue().matches(QueryParser.REGEX_NUMERAL)
                        && !values.get(i).getValue().equalsIgnoreCase("null")) {
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
                String v = values.get(i).getValue();
                if (!v.equalsIgnoreCase("null")) {
                    long parsed = Long.parseLong(v);
                    dataValueSet.add(parsed);
                } else {
                    dataValueSet.add("NULL"); // TODO :s
                }

            } else if (target.getColumnAt(i).type == Table.ColumnType.VARCHAR) { // if varchar

                LexicalToken tk = values.get(i);

                if (tk.getValue().equalsIgnoreCase("null") && !tk.isLiteral()) {
                    // add directly

                    dataValueSet.add("NULL");
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
                    if (!isReferenceTable) {
                        System.out.println("C1 CONSTRAINT FAILED");
                    }
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
                if (!isReferenceTable) {
                    System.out.println("C2 CONSTRAINT FAILED");
                }
                break;
            }
        }

        if (!interrupt && !isPKError) {
            // finally, insert the parsed
            target.insertRow(dataValueSet);
            if (!isReferenceTable) {
                System.out.println("RECORD INSERTED");
            }
            return true;
        }

        return false;
    }

    public static void createTable(String name, List<Table.Column> columns, String PK, ArrayList<String[]> FKDetails) throws CoSQLQueryExecutionError, CoSQLError {

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
        for (Table.Column fkCol : newTable.FKcolumns)
            initialCreateIndex("FKIndex: " + fkCol.getName(), newTable, fkCol);

        // user feedback
        String message = "TABLE CREATED";
        System.out.println(message);
    }

    private static void initialCreateIndex(String indexName, Table table, Table.Column column) {

//         instantiate new index and add to table
        table.initIndex();
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

    public static void update(String tableName, String colName, String rawComputeValue, String condition) throws CoSQLError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        if (table instanceof View) {
            View v = (View) table;
            updateItemsForView(v.getName(), colName, rawComputeValue, condition, false);
        } else {
            updateItems(table.getName(), colName, rawComputeValue,
                    new TupleCondition(condition, table.getName()).getContents(), false);
        }
    }


    public static boolean updateItemsForView(String viewName, String colName, String rawComputeValue
            , String condition, boolean isReferenceTable) throws CoSQLError {
        View view = (View) currentDatabase.getTable(viewName);

        if (view == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", viewName, currentDatabase);
        }

        if (!view.isPazira) {
            System.out.println(String.format("VIEW %s IS NOT UPDATABLE", viewName));
            return false;
        }

        boolean referenceErr = false;

        for (Table t : view.originalTables) {
            for (Column col : view.getColumns()) {
                if (col.name.equals(colName)) {
                    if (t instanceof View)
                        referenceErr = !updateItemsForView(t.getName(), colName, rawComputeValue, condition, true);
                    else
                        referenceErr = !updateItems(t.getName(), colName, rawComputeValue,
                                new TupleCondition(condition, t.getName()).getContents(), true);
                }
            }
        }

        if (referenceErr)
            return false;


        //checking if the wanted column is Fk and get its table reference

        ArrayList<Row> contents = new TupleCondition(condition, viewName).getContents();

        int colIndex = view.getColumnIndex(colName);

        ValueComputer.ParsedTuple tuple = ValueComputer.computeFieldBased(rawComputeValue, view);

        boolean error = false;

        for (Table.Row row : contents) {
            //LexicalToken computeValue = ComputeValue.compute(rawComputeValue, table, index);

            Object computeValue = tuple.computeForRow(row);
            if (colIndex == view.getColumnIndex(view.getPKcolumn())) {
                for (Table.Row r : view.getContents()) {
                    if (computeValue.equals(r.getValueAt(colIndex))) {
                        if (isReferenceTable)
                            System.out.println("C1 CONSTRAINT FAILED");
                        error = true;
                        break;
                    }
                }
            }
            if (!error)
                row.updateValueAt(colIndex, computeValue);
            else return false;
        }

        return true;
    }


    public static boolean updateItems(String tableName, String colName, String rawComputeValue, ArrayList<Table.Row> contents
            , boolean isReferenceTable) throws CoSQLError {

        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        //checking if the wanted column is Fk and get its table reference
        boolean isFK = false;
        Table refTable = null;
        int pkColIndex = -1;
        int i = -1;
        for (Table.Column c : table.FKcolumns) {
            i++;
            if (c.getName().equals(colName)) {
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
                        if (!isReferenceTable)
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

                            if (l.onUpdate.get(z).equalsIgnoreCase("restrict") && l.exists(obj, cool)) {
                                error = true;
                                if (!isReferenceTable)
                                    System.out.println("FOREIGN KEY CONSTRAINT RESTRICTS");
                                break;
                            }
                        }
                    }
                    if (error) {
                        continue;
                    }

                    int rowIndex = table.getRowIndex(row);
                    Object val = row.getValueAt(colIndex);
                    row.updateValueAt(colIndex, computeValue); //pk updates
                    table.updateIndexAt(colIndex, rowIndex, val);
                    // fk updates
                    for (Table target : table.listener) {
                        for (int j = 0; j < target.FKcolumns.size(); j++) {
                            if (target.tableReference.get(j).equals(table)) {
                                col = target.getColumnIndex(target.FKcolumns.get(j));
                                for (Table.Row r2 : target.getContents()) {
                                    if (r2.getValueAt(col).equals(obj)) {
                                        val = r2.getValueAt(col);
                                        r2.updateValueAt(col, computeValue);
                                        target.updateIndexAt(col, target.getRowIndex(r2), val);
                                    }
                                }
                            }
                        }
                    }
                }
            } else if (isFK) {

                if (!refTable.exists(computeValue, pkColIndex)) {
                    if (!isReferenceTable)
                        System.out.println("C2 CONSTRAINT FAILED");
//                    error = true; TODO ino bayad error begirim ya na ?
                } else {
                    int rowIndex = table.getRowIndex(row);
                    Object val = row.getValueAt(colIndex);
                    row.updateValueAt(colIndex, computeValue);
                    table.updateIndexAt(colIndex, rowIndex, val);
                }
            } else {
                int rowIndex = table.getRowIndex(row);
                Object val = row.getValueAt(colIndex);
                row.updateValueAt(colIndex, computeValue);
                table.updateIndexAt(colIndex, rowIndex, val);
            }
        }

        return !error;
    }

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


    public static void delete(String tableName, String condition) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        if (table instanceof View) {
            View v = (View) table;
            deleteItemsForView(v, condition, false);
        } else {
            deleteItems(table.getName(), new TupleCondition(condition, table.getName()).getContents(), false);
        }
    }


    public static boolean deleteItemsForView(View view, String condition, boolean isReferenceTable) throws CoSQLQueryExecutionError {
        if (!view.isPazira) {
            System.out.println(String.format("VIEW %s IS NOT UPDATABLE", view.getName()));
            return false;
        }

        //delete khodesh va refrence hash

        boolean error = false;
        for (Table t : view.originalTables) {
            if (t instanceof View) {
                View v = (View) t;
                error = !deleteItemsForView(v, condition, true);
            } else
                error = !deleteItems(t.getName(), new TupleCondition(condition, t.getName()).getContents(), true);
        }

        if (!error) {
            deleteItems(view.getName(), new TupleCondition(condition, view.getName()).getContents(), false);
            return true;
        }

        return false;
    }

    public static boolean deleteItems(String tableName, ArrayList<Table.Row> contentsMustBeDelete, boolean isReferenceTable) throws CoSQLQueryExecutionError {
        Table table = currentDatabase.getTable(tableName);

        if (table == null) {
            throwExecError("No table with name \'%s\' in database \'%s\'.", tableName, currentDatabase);
        }

        for (Table.Row row : new ArrayList<>(contentsMustBeDelete)) {
            if (!restrictDFS(row, table)) {
                deleteARow(row, table);
                for (TR d : deletedNodes) {
                    if (table.indexes != null) {
                        d.table.updateIndexForDelete(d.row);
                    }
                    d.table.removeRow(d.row);
                }
                deletedNodes.clear();
            } else {
                if (!isReferenceTable)
                    System.out.println("FOREIGN KEY CONSTRAINT RESTRICTS");
                return false; // TODO
            }
        }

        return true;
    }


    private static class GeneralSelectResult {

        Table finalResult;
        List<Row> sigmaList;
        Map<Row, Row> sigmaMap;

        public GeneralSelectResult(Table finalResult, List<Row> sigmaList, Map<Row, Row> sigmaMap) {
            this.finalResult = finalResult;
            this.sigmaList = sigmaList;
            this.sigmaMap = sigmaMap;
        }

    }

    public static GeneralSelectResult select(Table view, ArrayList<SelectValue> selectValues, String rawTupleCondition) throws CoSQLError {

        // get contents of tuple condition
        Map<Row, Row> sigmaMap = new HashMap<>();
        TupleCondition tupleCondition = new TupleCondition(rawTupleCondition, view.tableName);
        List<Table.Row> contents = tupleCondition.getContents();

        ArrayList<Integer> colIndexes = new ArrayList<>();
        ArrayList<Table.Column> columns = new ArrayList<>();

        // create header for new table (the table to be returned)
        for (SelectValue sv : selectValues) {

            if (sv.getType() == SelectValue.Type.COLUMN_NAME) {

                String colName = sv.getTargetColumn();
                colIndexes.add(view.getColumnIndex(colName));
                columns.add(view.getColumn(colName));

            } else {

                // check column name and type OK
                Column target = view.getColumn(sv.getTargetColumn());
                if (target.getType() != Table.ColumnType.INT)
                    throw new CoSQLError("Aggregation function " + sv.getAggregateMethod().getText() + " only allowed for INT columns.");

                String colName = sv.getAggregateMethod().getText() + "~" + sv.getTargetColumn();
                colIndexes.add(-1);
                columns.add(new Column(colName, Table.ColumnType.INT));
            }
        }

        ArrayList<Table.Row> finalContents = new ArrayList<>();
        for (Table.Row row : contents) {
            ArrayList<Object> values = new ArrayList<>();
            for (Integer i : colIndexes) {
                if (i != -1)
                    values.add(row.getValueAt(i));
                else
                    values.add("NULL"); //TODO :s
            }
            Row sigmaFiltered = new Table.Row(values);
            finalContents.add(sigmaFiltered);
            sigmaMap.put(sigmaFiltered, row);
        }

        Table res = new Table("printable", columns, finalContents);

        return new GeneralSelectResult(res, contents, sigmaMap);

    }

    public static Table select(ArrayList<String> tableNames, ArrayList<SelectValue> selectValues, String rawTupleCondition, int type, GroupByData groupBy) throws CoSQLError {

        Table table1 = currentDatabase.getTable(tableNames.get(0));

        // if query doesn't have joins or Cartesian multiplication
        if (tableNames.size() == 1) {

            // select both rows and columns
            GeneralSelectResult selected = select(table1, selectValues, rawTupleCondition);


            if (groupBy == null)
                return selected.finalResult;


            return  groupTable(selected.finalResult, groupBy, table1, selected.sigmaMap);
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

        // check selected columns are in group by, if it's a group query
        if (groupBy != null)
            for (SelectValue sv : selectValues) {
                if (sv.getType() == SelectValue.Type.COLUMN_NAME) {
                    if (!groupBy.getColumns().contains(sv.getTargetColumn())) {
                        throw new CoSQLError("Only aggregate functions and group columns allowed.");
                    }
                }
            }

        GeneralSelectResult selectResult = select(resultTable, selectValues, rawTupleCondition); // TODO TOFF :: bara zarb carthesian kaar nemikone :| think of something

        if (groupBy == null)
            return selectResult.finalResult;

        return groupTable(selectResult.finalResult, groupBy, resultTable, selectResult.sigmaMap);

    }

    public static Table groupTable(Table selectTable, GroupByData groupBy, Table sourceTable, Map<Row, Row> sigmaMap) throws CoSQLQueryExecutionError {

        // resolve columns
        List<Integer> groupColumnsPositions = new ArrayList<>();
        for (String colName : groupBy.getColumns()) {
            try {
                Column col = selectTable.getColumn(colName);
                groupColumnsPositions.add(selectTable.getColumnIndex(col));
            } catch (CoSQLError coSQLError) {
                coSQLError.printStackTrace();
                throw new CoSQLQueryExecutionError("Cannot resolve column name: " + colName);
            }
        }


        GroupHashMap<List<Object>, Row> map = new GroupHashMap<>();

        ArrayList<Row> contents = new ArrayList<>();

        // iterate table rows
        for (Row row : selectTable.getRows()) {

            // make a tuple of group columns' values
            List<Object> valuesTuple = new ArrayList<>();
            for (Integer idx : groupColumnsPositions) {
                // iterates through the group columns and adds
                // their values in rows to the tuple
                valuesTuple.add(row.getValueAt(idx));
            }

            map.add(valuesTuple, row);

        }

        for (List<Object> groupUniqueVal : map.keySet()) {

            Row firstRow = map.get(groupUniqueVal).get(0);
            int colsCount = selectTable.getColumns().size();

            ArrayList<Object> valuesTuple = new ArrayList<>(selectTable.getColumns().size());

            for (int i = 0; i < colsCount; i++) {

                Column column = selectTable.getColumnAt(i);

                if (column.getName().contains("~")) {

                    String[] explode = column.getName().split("~");
                    Method method = Method.fromText(explode[0]);

                    if (method == null) {
                        throw new CoSQLQueryExecutionError("No such aggregation function: " + column);
                    }

                    int targetIndex;

                    try {
                        targetIndex = sourceTable.getColumnIndex(explode[1]);
                    } catch (CoSQLError coSQLError) {
                        throw new CoSQLQueryExecutionError("Unknown column " + explode[1] + " used in aggregation function: " + explode[0]);
                    }

                    Object columnValue = null;

                    switch (method) {

                        case AVG: {

                            long sum = 0;
                            int count = 0;

                            for (Row row : map.get(groupUniqueVal)) {
                                Row corresponding = sigmaMap.get(row);
                                long value = (long) corresponding.getValueAt(targetIndex);
                                sum += value;
                                count += 1;
                            }

                            columnValue = sum / (double) count;
                            break;
                        }

                        case SUM: {

                            long sum = 0;

                            for (Row row : map.get(groupUniqueVal)) {
                                Row corresponding = sigmaMap.get(row);
                                long value = (long) corresponding.getValueAt(targetIndex);
                                sum += value;
                            }

                            columnValue = sum;
                            break;
                        }

                        case MAX: {

                            long max = Long.MIN_VALUE;

                            for (Row row : map.get(groupUniqueVal)) {
                                Row corresponding = sigmaMap.get(row);
                                long value = (long) corresponding.getValueAt(targetIndex);
                                max = Math.max(value, max);
                            }

                            columnValue = max;
                            break;
                        }

                        case MIN: {

                            long min = Long.MAX_VALUE;

                            for (Row row : map.get(groupUniqueVal)) {
                                Row corresponding = sigmaMap.get(row);
                                long value = (long) corresponding.getValueAt(targetIndex);
                                min = Math.min(value, min);
                            }

                            columnValue = min;
                            break;
                        }

                    } // end switch

                    valuesTuple.add(columnValue);

                } else {

                    valuesTuple.add(firstRow.getValueAt(i));

                }

            }

            Row row = new Row(valuesTuple);
            contents.add(row);
        }

        Table groupedTable = new Table("GROUP_RESULT", selectTable.columns, contents);

        if (groupBy.getRawHavingCondition() == null)
            return groupedTable;

        Table finalTable = new Table("final", selectTable.columns,
                new HavingCondition(groupBy.getRawHavingCondition(), selectTable, sourceTable, sigmaMap
                , map).getContents());

//        return new Table("GROUP_RESULT", selectTable.columns, contents);
        return finalTable;
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

        Table.Index idx = null;
        // fetch index if any
        if (table.indexes != null) {
            idx = table.indexes.get(column);
        }
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
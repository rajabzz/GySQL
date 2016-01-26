package dbms.engine;

import com.sun.rowset.internal.Row;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.util.StringUtils;

import java.io.Serializable;
import java.util.*;


public class Table implements Serializable {

    public enum ColumnType {
        INT, VARCHAR
    }

    public static class Column {

        String name;
        ColumnType type;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Column column = (Column) o;

            if (!name.equals(column.name)) return false;
            if (type != column.type) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }

        public Column(String name, String typeStr) throws CoSQLQueryParseError {

            Table.ColumnType type;

            // parse column type
            if (typeStr.equalsIgnoreCase("int")) {
                type = Table.ColumnType.INT;
            } else if (typeStr.equalsIgnoreCase("varchar")) {
                type = Table.ColumnType.VARCHAR;
            } else {
                String error = String.format("Unknown column type: %s", typeStr);
                throw new CoSQLQueryParseError(error);
            }

            this.type = type;
            this.name = name;

        }

        public Column(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public ColumnType getType() {
            return type;
        }
    }

    public static class Index {

        String name;
        Column column;

        TreeMap<Object, HashSet<Row>> index;

        public Index(String name, Column column) {

            this.name = name;
            this.column = column;

            if (column.type == ColumnType.INT) {

                this.index = new TreeMap<Object, HashSet<Row>>(new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return Long.compare((Long)o1, (Long)o2);
                    }
                });

            } else if (column.type == ColumnType.VARCHAR) {

                this.index = new TreeMap<Object, HashSet<Row>>(new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        if (o1 == o2) {
                            return 0;
                        }
                        if (o1 == null) {
                            return -1;
                        }
                        return ((String)o1).compareTo((String) o2);
                    }
                });

            } else {
                System.err.println("Bad column type in Index constructor");
            }
        }
    }

    /* primary and foreign key index*/
    int pkIndex;
    ArrayList<Table> refTables = new ArrayList<>();
    ArrayList<Integer> fkIndices = new ArrayList<>();

    /* name of the table */
    String tableName;

    /* table columns schema */
    ArrayList<Column> columns;

    Column pk = null;
    Column fk;

    /* table FKs */
    ArrayList<Column> FKcolumns = new ArrayList<>();

    /* tables that fks are referenced of */
    ArrayList<Table> tableReference = new ArrayList<>();

    /* tables that pk has references */
    ArrayList<Table> listener = new ArrayList<>();
    ArrayList<String> onUpdate = new ArrayList<>();
    ArrayList<String> onDelete = new ArrayList<>();

    /* table contents */
    ArrayList<Row> contents;

    /* indexes */
    HashMap<Column, Index> indexes;

    /* default constructor */
    public Table(String name) {
        this.tableName = name;
        this.columns = new ArrayList<>();
        this.contents = new ArrayList<>();
        this.indexes = new HashMap<>();
    }

    public Table(String tableName, ArrayList<Column> columns, ArrayList<Row> contents) {
        this.tableName = tableName;
        this.columns = columns;
        this.contents = contents;
    }

    public String getName() {
        return tableName;
    }

    public void setPKcolumn(String pkName) throws CoSQLError {
        pk = getColumn(pkName);
    }

    public Column getPKcolumn() {
        return pk;
    }

    public Column setFKcolumn(String fkName) throws CoSQLError {
        return fk = getColumn(fkName);
    }

    public Column getFKcolumn() {
        return fk;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public Column getColumnAt(int i) {
        return columns.get(i);
    }

    public Column getColumn(String colName) throws CoSQLError {
        for (Column col: columns) {
            if (col.name.equals(colName))
                return col;
        }
        throw new CoSQLError("No such column found!");
    }

    public ArrayList<Row> getContents() {
        return contents;
    }

    public int getColumnIndex(String colName) throws CoSQLError {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name.equals(colName))
                return i;
        }
        throw new CoSQLError("No such column '%s' found!", colName);
    }

    public int getColumnIndex(Column col) {
        return columns.indexOf(col);
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public void insertRow(ArrayList<Object> args) {

//        int i = getColumnIndex(pk);
//        for (Row prev : contents) {
//            if (prev.getValueAt(i).equals(args.get(i))) {
//                System.out.println("C1 CONSTRAINT FAILED");
//                return;
//            }
//        }
//        if (args.get(i) == null) {
//            System.out.println("C1 CONSTRAINT FAILED");
//            return;
//
//        } else {
        Row newRow = new Row(args);
        this.contents.add(newRow);

        for (Index index: indexes.values()) {
            indexRow(newRow, index);

        }
    }

    void indexRow(Row row, Index index) {

        // get index's column position
        Column c = index.column;
        int pos = getColumnIndex(c);

        // get value
        Object value = row.getValueAt(pos);

        // get index list or create
        HashSet<Row> indexRowsOnValue = index.index.get(value);
        if (indexRowsOnValue == null) {
            indexRowsOnValue = new HashSet<>();
            index.index.put(value, indexRowsOnValue);
        }

        indexRowsOnValue.add(row);

    }

    public Iterable<Row> getRows() {
        return contents;
    }

    public int getRowCount() {
        return contents.size();
    }

    public Row getRowAt(int i) {
        return contents.get(i);
    }

    public int getRowIndex(Row row) {
        return contents.indexOf(row);
    }

    public void removeRow(Row row) {
        this.contents.remove(row);
    }

    public void addColumn(String name, ColumnType type) {
        columns.add(new Column(name, type));
    }

    public void addColumn(Column c) {
        columns.add(c);
    }

    public void addAllColumns(Collection<Column> cols) {
        for (Column c: cols)
            columns.add(c);
    }

    public void addIndex(Index index) {
        indexes.put(index.column, index);
    }

    public void updateIndexAt(int colIndex, int rowIndex) {

        Column column = getColumnAt(colIndex);
        Row row = getRowAt(rowIndex);
        Object value = row.getValueAt(colIndex);

        Index idx = indexes.get(column);

        if (idx == null) {
            return;
        }

        HashSet<Row> vals = idx.index.get(value);
        vals.remove(row);

        indexRow(row, idx);
    }

    public void updateIndexForDelete(Row row) {

        //Index idx = indexes.get(getColumnAt(colIndex));

        for (Index idx : indexes.values()) {

            int colIndex = getColumnIndex(idx.column);

            HashSet<Row> set = idx.index.get(row.getValueAt(colIndex));
            set.remove(row);

            if (set.isEmpty()) {
                idx.index.remove(row.getValueAt(colIndex));
            }
        }

    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            result.append((columns.get(i).getName()).substring(columns.get(i).getName().indexOf(".") + 1) );
            if (i != columns.size() - 1) {
                result.append(",");
            }
        }
        for (Row row : contents) {
            result.append("\n").append(row);
        }
        if (contents.size() == 0)
            result = new StringBuilder("NO RESULTS");
        return result.toString();
    }

    public int getFkIndex(Table ref) {

        for (int i = 0; i < tableReference.size(); i++) {
            if (ref.equals(tableReference.get(i))) {
                return ref.getColumnIndex(FKcolumns.get(i));
            }
        }
        return -1;
    }

    public boolean exists(Object value, int column) {

        for (Row r : contents) {
            if (r.getValueAt(column).equals(value))
                return true;
        }
        return false;
    }

    public static class Row implements Serializable {

        ArrayList<Object> values;

        public Row(ArrayList<Object> values) {
            this.values = values;
        }

        public ArrayList<Object> getValues() {
            return values;
        }

        public void updateValueAt(int i, Object obj) {
            values.remove(i);
            values.add(i, obj);
        }

        public Object getValueAt(int index) {
            return values.get(index);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Row row = (Row) o;

            if (values != null ? !values.equals(row.values) : row.values != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return values != null ? values.hashCode() : 0;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                result.append(values.get(i));
                if (i != values.size() - 1) {
                    result.append(",");
                }
            }

            return result.toString();
        }
    }

    private static boolean containsIgnoreCase (ArrayList<String> list, String element) {
        /***
         * contains method with IgnoreCase
         */
        for (String s : list) {
            if (s.equalsIgnoreCase(element))
                return true;
        }
        return false;
    }

    private ArrayList<String> getColumnNames () {
        ArrayList<String> res = new ArrayList<>();
        for (Column c : columns)
            res.add(c.getName());
        return res;
    }

    public Table cartesianProduct (Table other) {
        /* queryColNames are the columns in query should be saved by their table name */
        Table result = new Table("cartesTable");
        // TODO error if queryColNames.size > 2

        ArrayList<Column> resCol = new ArrayList<>();
        for (Column c : this.columns) {
            String fullColName = this.tableName + "." + c.getName();
            resCol.add(new Column(fullColName, c.type));
        }

        for (Column c : other.columns) {
            String fullColName = other.tableName + "." + c.getName();
            resCol.add(new Column(fullColName, c.type));
        }
        result.addAllColumns(resCol);

        for (Row r : this.contents) {
            for (Row r2 : other.contents) {
                ArrayList<Object> vals = new ArrayList<>();
                vals.addAll(r.getValues());
                vals.addAll(r2.getValues());
                Row newRow = new Row(vals);
                result.contents.add(newRow);
            }
        }
        return result;
    }

    public Table join (Table other) {
        // this has refrence to other this.fk = other.pk
        Table result = new Table("joinTable");
        // TODO error if queryColNames.size > 2

        ArrayList<Column> resCol = new ArrayList<>();
        for (Column c : this.columns) {
            String fullColName = this.tableName + "." + c.getName();
            resCol.add(new Column(fullColName, c.type));
        }
        for (Column c : other.columns) {
            String fullColName = other.tableName + "." + c.getName();
            resCol.add(new Column(fullColName, c.type));
        }
        result.addAllColumns(resCol);
        int fkIndex = this.getColumnIndex(FKcolumns.get(tableReference.indexOf(other)));
        for (Row r : contents) {
            ArrayList<Object> vals = new ArrayList<>();
            vals.addAll(r.getValues());
            Object key = r.getValueAt(fkIndex);
            Column fafaf = getPKcolumn();
            Index idx = other.indexes.get(fafaf);
            HashSet<Table.Row> indexedResult = idx.index.get(key);
            ArrayList<Table.Row> resultRows = new ArrayList<>();
            if (indexedResult == null)
                continue; // mapping does not exist
            else
                resultRows.addAll(indexedResult);
            // TODO error if resRows.size > 1 (every fk map to one pk)
            Row mappedRow = resultRows.get(0);
            vals.addAll(mappedRow.getValues());
            // TODO error if vals.size != this.colCount + other.colCount (pk)
            Row newRow = new Row(vals);
            result.contents.add(newRow);
        }
        return result;
    }

}
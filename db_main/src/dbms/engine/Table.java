package dbms.engine;

import com.sun.rowset.internal.Row;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryParseError;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Created by blackvvine on 10/25/15.
 */
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

        public String getName() {return name;}

        public ColumnType getType() {return type;}
    }

    public static class Index {

        String name;
        Column column;

        HashMap<Object, HashSet<Row>> index;

        public Index(String name, Column column) {
            this.name = name;
            this.column = column;
            this.index = new HashMap<>();
        }
    }

    /* name of the table */
    String tableName;

    /* table columns schema */
    ArrayList<Column> columns;

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
        throw new CoSQLError("No such column found!");
    }

    public int getColumnIndex(Column col) {
        return columns.indexOf(col);
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public void insertRow(ArrayList<Object> args) {

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

    public int getRowCount() { return contents.size(); }

    public Row getRowAt(int i) {return contents.get(i);}

    public int getRowIndex(Row row) {
        return contents.indexOf(row);
    }

    public void removeRow(Row row) {this.contents.remove(row);}

    public void addColumn(String name, ColumnType type) {
        columns.add(new Column(name, type));
    }

    public void addColumn(Column c) {
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

        for (Index idx: indexes.values()) {

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
            result.append(columns.get(i).name);
            if (i != columns.size() - 1) {
                result.append(",");
            }
        }
        for (Row row: contents) {
            result.append("\n").append(row);
        }
        if (contents.size() == 0)
            result = new StringBuilder("NO RESULTS");
        return result.toString();
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
                    result.append(", ");
                }
            }

            return result.toString();
        }
    }

}

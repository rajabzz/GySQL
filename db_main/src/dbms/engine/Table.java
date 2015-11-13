package dbms.engine;

import com.sun.rowset.internal.Row;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryParseError;

import java.io.Serializable;
import java.util.ArrayList;

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

    /* name of the table */
    String tableName;

    /* table columns schema */
    ArrayList<Column> columns;

    /* table contents */
    ArrayList<Row> contents;

    /* default constructor */
    public Table(String name) {
        this.tableName = name;
        this.columns = new ArrayList<>();
        this.contents = new ArrayList<>();
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

    public void insertRow(ArrayList args) {
        this.contents.add(new Row(args));
    }

    public int getRowCount() { return columns.size(); }

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

}

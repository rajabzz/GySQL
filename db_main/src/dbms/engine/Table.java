package dbms.engine;

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
    };

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

    public int getColumnCount() {
        return columns.size();
    }

    public Column getColumnAt(int i) {
        return columns.get(i);
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public void insertRow(ArrayList args) {
        this.contents.add(new Row(args));
    }

    public void addColumn(String name, ColumnType type) {
        columns.add(new Column(name, type));
    }

    public void addColumn(Column c) {
        columns.add(c);
    }

    private class Row implements Serializable {

        ArrayList<Object> values;

        public Row(ArrayList<Object> values) {
            this.values = values;
        }
    }

}

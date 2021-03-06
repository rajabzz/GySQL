package dbms.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by blackvvine on 10/25/15.
 */
public class Database implements Serializable {

    String name;

    Authority owner;

    HashMap<String, Table> tables;

    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
    }

    public void addTable(Table table) {
        tables.put(table.tableName, table);
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public void removeTable(Table table) {
        tables.remove(table.tableName);
    }
}
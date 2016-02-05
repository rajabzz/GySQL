package dbms.engine;

import java.util.ArrayList;

/**
 * Created by negin on 2/2/16.
 */
public class View extends Table {
    public boolean isPazira = true;
    public ArrayList<Table> originalTables = new ArrayList<>();


    public View(String tableName, ArrayList<Column> columns, ArrayList<Row> contents) {
        super(tableName, columns, contents);
    }

    public View(String name) {
        super(name);
    }
}
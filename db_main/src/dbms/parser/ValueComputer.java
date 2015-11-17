package dbms.parser;

import dbms.engine.Table;

/**
 * Created by blackvvine on 11/17/15.
 */
public class ValueComputer {

    public enum ValueType {
        CONSTANT, FIELD_BASED
    }

    public static class ParsedTuple {

        public Object computeForRow(Table.Row row) {

        }
    }


    public static ValueType getType(String rawInput) {
        // TODO
    }

    public static Object computeConstant(String rawInput) {
        // TODO
    }


}

package dbms.parser;

import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryParseError;

public class ComputeValue {

    /**
     * Author: Fucking Molla
     */
    static final String delimiters = "+-*/";

    public LexicalToken compute(String rawInput, Table table,int idx) throws CoSQLQueryParseError{

        LexicalToken dummy = new LexicalToken("", true);
        LexicalToken result = dummy;
        LexicalToken first  = dummy;
        LexicalToken second = dummy;

        int index = rawInput.length() - 1;

        while (index >= 0) {
            if (delimiters.contains("" + rawInput.charAt(index))) {
                first = compute( rawInput.substring(0, index), table, idx );
                second = singleTokenOperate( rawInput.substring(index + 1), table, idx );
                break;
            }
            index--;
        }
        if (index == -1) {
            result = singleTokenOperate(rawInput, table, idx);
        } else {

            if ( (!first.literal) && (!second.literal) ) {

                int firstVal = Integer.valueOf(first.value);
                int secondVal = Integer.valueOf(second.value);

                switch(rawInput.charAt(index)) {
                    case '+':
                        result = new LexicalToken(String.valueOf(firstVal + secondVal), false);
                        break;
                    case '-':
                        result = new LexicalToken(String.valueOf(firstVal - secondVal), false);
                        break;
                    case '*':
                        result = new LexicalToken(String.valueOf(firstVal * secondVal), false);
                        break;
                    default:    // case '/'
                        result = new LexicalToken(String.valueOf(firstVal / secondVal), false);
                }
            } else {
                // other than number computing
                result = new LexicalToken(first.value + second.value, true);
            }
        }
        return result;
    }

    private LexicalToken singleTokenOperate(String rawToken, Table table, int index) throws CoSQLQueryParseError{
        // rawToken is INT or String between 's or "s
        if (rawToken.contains("\'") || rawToken.contains("\"")) {
            return new LexicalToken(rawToken.substring(1, rawToken.length()-1), true);
        } else{
            try {
                long num = Long.parseLong(rawToken);
                return new LexicalToken(rawToken, false);
            } catch (NumberFormatException e) {
                return returnField(rawToken, table, index);
            }
        }
    }

    private LexicalToken returnField(String rawInput, Table table, int index) throws CoSQLQueryParseError {

        // rawInput is just the column name
        for (int i = 0; i < table.getColumnCount(); i++) {

            if (table.getColumnAt(i).getName().equals(rawInput)) {
                Object record = table.getRowAt(index).getValueAt(i);

                if (table.getColumnAt(i).getType() == Table.ColumnType.INT) {
                    return new LexicalToken( (long)record + "" ,  false);
                } else {
                    return new LexicalToken( (String) record ,  true);
                }

            }
        }
        throw new CoSQLQueryParseError("no column found in this table");
    }
}
package dbms.parser;

import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.exceptions.EndOfBufferException;
import dbms.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

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


    public static ValueType getType(String rawInput) throws CoSQLQueryParseError {
        final String delimiters = "+-*/=";

        ParseData parsedData = new ParseData(rawInput);
        boolean hasField = false;
        while (parsedData.hasNext()) {
            LexicalToken token = parsedData.nextFullToken();

            if (!delimiters.contains(token.getValue())
                    && !token.isLiteral()
                    && token.getValue().matches("^\\d+$"))
                hasField = true;
        }

        if (hasField)
            return ValueType.FIELD_BASED;

        else return ValueType.CONSTANT;
    }

    public static Object computeConstant(String rawInput) throws CoSQLQueryParseError {
        ParseData parseData = new ParseData(rawInput);
        boolean hasString = false;
        while (parseData.hasNext()) {
            LexicalToken token = parseData.nextFullToken();
            if (token.isLiteral()) {
                hasString = true;
            }
        }

        parseData.goToFirst();
        if (hasString)
            return computeConstantString(parseData);
        return computeConstantNumber(parseData);
    }

    private static Object computeConstantString(ParseData parseData) throws EndOfBufferException {
        StringBuilder sb = new StringBuilder();
        while (parseData.hasNext()) {
            sb.append(parseData.next());
        }
        return sb.toString();
    }

    private static Object computeConstantNumber(ParseData parseData) throws EndOfBufferException {
        final int OPERATOR_PLUS = 0;
        final int OPERATOR_MINUS = 1;
        final int OPERATOR_MULTIPLY = 2;
        final int OPERATOR_DIVIDE = 3;
        final int OPERATOR_NOTHING = 4;

        int operatorType = OPERATOR_NOTHING;

        long result = 0;

        while (parseData.hasNext()) {
            String token = parseData.next();
            if (token.contains("+"))
                operatorType = OPERATOR_PLUS;
            else if (token.contains("-"))
                operatorType = OPERATOR_MINUS;
            else if (token.contains("*"))
                operatorType = OPERATOR_MULTIPLY;
            else if (token.contains("/"))
                operatorType = OPERATOR_DIVIDE;
            else if (operatorType == OPERATOR_NOTHING)
                result = Long.parseLong(token);
            else if (operatorType == OPERATOR_PLUS)
                result += Long.parseLong(token);
            else if (operatorType == OPERATOR_MINUS)
                result -= Long.parseLong(token);
            else if (operatorType == OPERATOR_MULTIPLY)
                result *= Long.parseLong(token);
            else result /= Long.parseLong(token);
        }

        return result;
    }

    public static ParsedTuple computeFieldBased(String rawInput) {
        // TODO
    }

    private static class ParseData {

        private List<LexicalToken> tokens;
        private int next;
        private ArrayList<CoSQLCommand> commands = new ArrayList<>();

        LexicalToken nextFullToken() throws EndOfBufferException {
            if (next >= tokens.size()) {
                throw new EndOfBufferException();
            }
            return tokens.get(next++);
        }

        String next() throws EndOfBufferException {
            if (next >= tokens.size()) {
                throw new EndOfBufferException();
            }
            return tokens.get(next++).getValue();
        }

        void goPrev() {
            if (next != 0)
                next--;
        }

        void goToLast() {
            next = tokens.size() - 1;
        }

        void goToFirst() {next = 0;}

        LexicalToken peek() {
            return tokens.get(next);
        }

        boolean hasNext() {
            return next < tokens.size();
        }

        String rest() {
            StringBuilder sb = new StringBuilder();
            for (int i=next; i<tokens.size(); i++) {
                sb.append(tokens.get(i));
                if (i != tokens.size() - 1) {
                    sb.append(" ");
                }
            }
            return sb.toString();
        }

        ParseData(String command) throws CoSQLQueryParseError {
            tokens = StringUtils.tokenizeQuery(command);
            next = 0;
        }
    }

}

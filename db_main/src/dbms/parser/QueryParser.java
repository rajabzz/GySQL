package dbms.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import dbms.UserInterface;
import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.exceptions.EndOfBufferException;
import dbms.exceptions.EndOfSessionException;
import dbms.util.StringUtils;
import static dbms.engine.Table.Column;
import static dbms.util.LanguageUtils.throwParseError;

/**
 * Author: Iman Akbari
 */
public class QueryParser {

    public static final String REGEX_COLUMN_NAME = "^[a-zA-Z_\\$][a-zA-Z0-9_\\$]*$";
    public static final String REGEX_TABLE_NAME = "^[a-zA-Z_\\$][a-zA-Z0-9_\\$]*$";
    public static final String REGEX_DATABASE_NAME = "^[a-zA-Z\\$][a-zA-Z_\\$0-9]*$";
    public static final String REGEX_NUMERAL = "^[\\+\\-]?[0-9]+$";

    private UserInterface userInterface;
    private ParseData parseData;

    public QueryParser(UserInterface userInterface) {
        this.userInterface = userInterface;
    }

    public void parseAndRun(String query) throws EndOfSessionException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        // checking directives, currently message passing is only
        // through exceptions
        checkDirectives(query);

        // parse query, throw in case of errors, execute if non
        parseData = new ParseData(query);

        // start variable (CFG-ish)
        start(parseData);

    }

    // TODO else error for all these

    private void start(ParseData parseData) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String next = parseData.next();
        if (next.equalsIgnoreCase("create")) {
            create(parseData);
        } else if (next.equalsIgnoreCase("insert")) {
            insert(parseData);
        } else {
            // TODO error
        }

        end(parseData);
    }

    private void insert(ParseData parseData) throws CoSQLQueryParseError {

        String lookAhead = parseData.next();

        // force INTO keyword after INSERT
        if (!lookAhead.equalsIgnoreCase("into")) {
            String message = String.format("Expected INTO after INSERT, before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(message);
        }

        String tableName = tableName(parseData);

        // mandatory VALUES keyword
        lookAhead = parseData.next();

        if (!lookAhead.equalsIgnoreCase("values")) {
            String error = String.format("Expected VALUES before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        // open parenthesis
        lookAhead = parseData.next();

        if (!lookAhead.equals("(")) {
            String error = String.format("Expected '(' before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        // iterate through values and parse so that it can be passed
        // to core database
        ArrayList<LexicalToken> values = new ArrayList<>();
        boolean expectComma = false;

        while (true) {

            LexicalToken token = parseData.nextFullToken();

            if (expectComma) {

                if (token.getValue().equals(",")) {
                    expectComma = false;
                } else if (token.getValue().equals(")")) { // break if end
                    break;
                } else {
                    String message = String.format("Expected comma or ')' before \'%s\'", token);
                    throw new CoSQLQueryParseError(message);
                }

            } else {

                if (token.getValue().matches(REGEX_NUMERAL) || token.isLiteral() || token.getValue().equalsIgnoreCase("null")) {
                    values.add(token);
                    expectComma = true;
                } else {
                    String message = String.format("Unexpected token: \'%s\' in values.", token.getValue());
                    throw new CoSQLQueryParseError(message);
                }

            }

        } // end while loop

        // create and add command to batch
        CoSQLInsert insertQuery = new CoSQLInsert(tableName, values);
        parseData.addCommand(insertQuery);

    }

    private void update(ParseData parseData) throws CoSQLQueryParseError {

        // get table name
        String tableName = tableName(parseData);

        // force SET keyword
        String lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase("set")) {
            throwParseError("Expected keyword SET before %s", lookAhead);
        }

        // get field name
        String columnName = columnName(parseData);

        // force the '=' character in between
        lookAhead = parseData.next();
        if (!lookAhead.equals("=")) {
            throwParseError("Unexpected \'%s\', expecting =", lookAhead);
        }

        // new value for field indicated earlier
        //TODO COMPUTE_VALUE
        String value = parseData.next();

        // force WHERE keyword
        lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase("where")) {
            throwParseError("Expected keyword WHERE before %s", lookAhead);
        }

        // TODO TUPLE_CONDITION
        String condition = parseData.next();

    }

    private String tableName(ParseData parseData) throws CoSQLQueryParseError {

        // get potential table name
        String tableName = parseData.next();

        // match regex
        if (!tableName.matches(REGEX_TABLE_NAME)) {
            String error = String.format("Illegal table name: '%s'", tableName);
            throw new CoSQLQueryParseError(error);
        }

        return tableName;
    }

    private void create(ParseData parseData) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String lookAhead = parseData.next();

        if (lookAhead.equalsIgnoreCase("database")) {
            createDatabase(parseData);
        } else if (lookAhead.equalsIgnoreCase("table")) {
            createTable(parseData);
        } else {
            String error = String.format("Unexpected \'%s\' after CREATE.", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

    }

    private void createDatabase(ParseData parseData) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String name = parseData.next();

        if (!name.matches(REGEX_DATABASE_NAME)) {
            String error = String.format("Illegal name for database: \'%s\'", name);
            throw new CoSQLQueryParseError(error);
        }

        parseData.addCommand(new CoSQLCreateDatabase(name));
    }

    private void createTable(ParseData parseData) throws CoSQLQueryParseError, CoSQLQueryExecutionError {

        String name = tableName(parseData);

        String lookAhead = parseData.next();

        if (!lookAhead.equalsIgnoreCase("(")) {
            String error = String.format("Expected '(' before %s", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        ArrayList<Column> columns = new ArrayList<>();

        Column first = tableColumn(parseData); // TODO proper error report while throwing
        columns.add(first);

        while (true) {
            lookAhead = parseData.next();
            if (lookAhead.equals(")")) {
                break;
            } else if (lookAhead.equals(",")) {
                Column column = tableColumn(parseData);
                columns.add(column);
            } else {
                String error = String.format("Unexpected syntax near \'%s\'", lookAhead);
                throw new CoSQLQueryParseError(error);
            }
        }

        CoSQLCommand command = new CoSQLCreateTable(name, columns);
        parseData.addCommand(command);
    }

    private Table.Column tableColumn(ParseData parseData) throws CoSQLQueryParseError {

        String columnName = columnName(parseData);

        String columnType = parseData.next();

        if (!columnType.matches("^[a-zA-Z]*$")) {
            String error = String.format("Column type '%s' doesn't match legal pattern.", columnType);
            throw new CoSQLQueryParseError(error);
        }

        return new Column(columnName, columnType);
    }

    private String columnName(ParseData parseData) throws CoSQLQueryParseError {

        String columnName = parseData.next();

        if (!columnName.matches(REGEX_COLUMN_NAME)) {
            String error = String.format("Illegal table column name: '%s'", columnName);
            throw new CoSQLQueryParseError(error);
        }

        return columnName;
    }

    private void end(ParseData parseData) throws CoSQLQueryParseError, CoSQLQueryExecutionError {

        String eoq = parseData.next();

        if (!eoq.equals(";")) {
            String error = String.format("Expected semicolon before: \'%s\'", eoq);
            throw new CoSQLQueryParseError(error);
        }

        if (parseData.hasNext()) {
            String error = String.format("Unexpected \'%s\' at the end of query. Expecting query's end.", parseData.rest());
            throw new CoSQLQueryParseError(error);
        }

        // trigger parsed query run
        parseData.batchRun();
    }

    private void checkDirectives(String query) throws EndOfSessionException {
        if (query.matches("^\\s*(exit|EXIT|quit|QUIT|finish|FINISH|tamoom)\\s*$")) {
            throw new EndOfSessionException();
        }
    }

    /**
     * helper class for query pare flow
     */
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

        boolean addCommand(CoSQLCommand command) {
            if (!commands.contains(command)) {
                commands.add(command);
                return true;
            }
            return false;
        }

        void batchRun() throws CoSQLQueryExecutionError {
            for (CoSQLCommand command: commands) {
                command.execute(); // TODO batch run might get fucked if something goes wrong in the middle, proper revert system needed
            }
        }

        /**
         * splits the command into separate tokens
         *
         * @param command
         */
        ArrayList<String> tokenize(String command) {
            String[] bySpace = command.split("\\s");
            ArrayList<String> res = new ArrayList<>();
            for (String string: bySpace) {
                StringTokenizer st = new StringTokenizer(string, "(),;", true);
            }
            return res;
        }

        ParseData(String command) throws CoSQLQueryParseError {
            tokens = StringUtils.tokenizeQuery(command);
            next = 0;
        }
    }

}

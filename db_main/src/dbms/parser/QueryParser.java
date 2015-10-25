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
import dbms.util.LanguageUtils;
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
    private Buffer buffer;

    public QueryParser(UserInterface userInterface) {
        this.userInterface = userInterface;
    }

    public void parseAndRun(String query) throws EndOfSessionException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        // checking directives, currently message passing is only
        // through exceptions
        checkDirectives(query);

        // parse query, throw in case of errors, execute if non
        buffer = new Buffer(query);

        // start variable (CFG-ish)
        start(buffer);

    }

    // TODO else error for all these

    private void start(Buffer buffer) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String next = buffer.next();
        if (next.equalsIgnoreCase("create")) {
            create(buffer);
        } else if (next.equalsIgnoreCase("insert")) {
            insert(buffer);
        } else {
            // TODO error
        }

        end(buffer);
    }

    private void insert(Buffer buffer) throws CoSQLQueryParseError {

        String lookAhead = buffer.next();

        // force INTO keyword after INSERT
        if (!lookAhead.equalsIgnoreCase("into")) {
            String message = String.format("Expected INTO after INSERT, before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(message);
        }

        String tableName = tableName(buffer);

        // mandatory VALUES keyword
        lookAhead = buffer.next();

        if (!lookAhead.equalsIgnoreCase("values")) {
            String error = String.format("Expected VALUES before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        // open parenthesis
        lookAhead = buffer.next();

        if (!lookAhead.equals("(")) {
            String error = String.format("Expected '(' before \'%s\'", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        // iterate through values and parse so that it can be passed
        // to core database
        ArrayList<LexicalToken> values = new ArrayList<LexicalToken>();
        boolean expectComma = false;

        while (true) {

            LexicalToken token = buffer.nextFullToken();

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

                if (token.getValue().matches(REGEX_NUMERAL) || token.isLiteral()) {
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
        buffer.addCommand(insertQuery);

    }

    private void update(Buffer buffer) throws CoSQLQueryParseError {

        // get table name
        String tableName = tableName(buffer);

        // force SET keyword
        String lookAhead = buffer.next();
        if (!lookAhead.equalsIgnoreCase("set")) {
            throwParseError("Expected keyword SET before %s", lookAhead);
        }

        // get field name
        String columnName = columnName(buffer);

        // force the '=' character in between
        lookAhead = buffer.next();
        if (!lookAhead.equals("=")) {
            throwParseError("Unexpected \'%s\', expecting =", lookAhead);
        }

        // new value for field indicated earlier
        String value = buffer.next();

    }

    private String tableName(Buffer buffer) throws CoSQLQueryParseError {

        // get potential table name
        String tableName = buffer.next();

        // match regex
        if (!tableName.matches(REGEX_TABLE_NAME)) {
            String error = String.format("Illegal table name: '%s'", tableName);
            throw new CoSQLQueryParseError(error);
        }

        return tableName;
    }

    private void create(Buffer buffer) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String lookAhead = buffer.next();

        if (lookAhead.equalsIgnoreCase("database")) {
            createDatabase(buffer);
        } else if (lookAhead.equalsIgnoreCase("table")) {
            createTable(buffer);
        } else {
            String error = String.format("Unexpected \'%s\' after CREATE.", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

    }

    private void createDatabase(Buffer buffer) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String name = buffer.next();

        if (!name.matches(REGEX_DATABASE_NAME)) {
            String error = String.format("Illegal name for database: \'%s\'", name);
            throw new CoSQLQueryParseError(error);
        }

        buffer.addCommand(new CoSQLCreateDatabase(name));
        //end(buffer);
    }

    private void createTable(Buffer buffer) throws CoSQLQueryParseError, CoSQLQueryExecutionError {

        String name = tableName(buffer);

        String lookAhead = buffer.next();

        if (!lookAhead.equalsIgnoreCase("(")) {
            String error = String.format("Expected '(' before %s", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

        ArrayList<Column> columns = new ArrayList<Column>();

        Column first = tableColumn(buffer); // TODO proper error report while throwing
        columns.add(first);

        while (true) {
            lookAhead = buffer.next();
            if (lookAhead.equals(")")) {
                break;
            } else if (lookAhead.equals(",")) {
                Column column = tableColumn(buffer);
                columns.add(column);
            } else {
                String error = String.format("Unexpected syntax near \'%s\'", lookAhead);
                throw new CoSQLQueryParseError(error);
            }
        }

        CoSQLCommand command = new CoSQLCreateTable(name, columns);
        buffer.addCommand(command);

        //end(buffer);
    }

    private Table.Column tableColumn(Buffer buffer) throws CoSQLQueryParseError {

        String columnName = columnName(buffer);

        String columnType = buffer.next();

        if (!columnType.matches("^[a-zA-Z]*$")) {
            String error = String.format("Column type '%s' doesn't match legal pattern.", columnType);
            throw new CoSQLQueryParseError(error);
        }

        return new Column(columnName, columnType);
    }

    private String columnName(Buffer buffer) throws CoSQLQueryParseError {

        String columnName = buffer.next();

        if (!columnName.matches(REGEX_COLUMN_NAME)) {
            String error = String.format("Illegal table column name: '%s'", columnName);
            throw new CoSQLQueryParseError(error);
        }

        return columnName;
    }

    private void end(Buffer buffer) throws CoSQLQueryParseError, CoSQLQueryExecutionError {

        String eoq = buffer.next();

        if (!eoq.equals(";")) {
            String error = String.format("Expected semicolon before: \'%s\'", eoq);
            throw new CoSQLQueryParseError(error);
        }

        if (buffer.hasNext()) {
            String error = String.format("Unexpected \'%s\' at the end of query. Expecting query's end.", buffer.rest());
            throw new CoSQLQueryParseError(error);
        }

        // trigger parsed query run
        buffer.batchRun();
    }

    private void checkDirectives(String query) throws EndOfSessionException {
        if (query.matches("^\\s*(exit|quit|finish|tamoom)\\s*$")) {
            throw new EndOfSessionException();
        }
    }

    /**
     * helper class for query pare flow
     */
    private static class Buffer {

        private List<LexicalToken> tokens;
        private int next;
        private ArrayList<CoSQLCommand> commands = new ArrayList<CoSQLCommand>();

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
            ArrayList<String> res = new ArrayList<String>();
            for (String string: bySpace) {
                StringTokenizer st = new StringTokenizer(string, "(),;", true);
            }
            return res;
        }

        Buffer(String command) throws CoSQLQueryParseError {
            tokens = StringUtils.tokenizeQuery(command);
            next = 0;
        }
    }

}

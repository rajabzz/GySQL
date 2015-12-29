package dbms.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import dbms.UserInterface;
import dbms.engine.Table;
import dbms.exceptions.*;
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
    public static final String REGEX_INDEX_NAME = "^[a-zA-Z_\\$][a-zA-Z0-9_\\$]*$";

    private UserInterface userInterface;
    private ParseData parseData;

    public QueryParser(UserInterface userInterface) {
        this.userInterface = userInterface;
    }

    public void parseAndRun(String query) throws EndOfSessionException, CoSQLError {

        // checking directives, currently message passing is only
        // through exceptions
        checkDirectives(query);

        // parse query, throw in case of errors, execute if non
        parseData = new ParseData(query);

        // start variable (CFG-ish)
        start(parseData);

    }

    // TODO else error for all these

    private void start(ParseData parseData) throws CoSQLError {

        try {

            String next = parseData.next();
            if (next.equalsIgnoreCase("create")) {
                create(parseData);
            } else if (next.equalsIgnoreCase("insert")) {
                insert(parseData);
            } else if (next.equalsIgnoreCase("update")) {
                update(parseData);
            } else if (next.equalsIgnoreCase("select")) {
                select(parseData);
            } else if (next.equalsIgnoreCase("delete")) {
                delete(parseData);
            } else if (next.equalsIgnoreCase("print")) { // for debugging only
                print(parseData);
            } else {
                // TODO error
                System.err.println("Invalid command!");
            }

            end(parseData);

        } catch (EndOfBufferException e) {
            System.err.println("Unexpected end of input");
        }

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

        // compute value
        String computeValueStr = "";
        LexicalToken nextFullToken;
        while (!(nextFullToken = parseData.nextFullToken()).getValue().equalsIgnoreCase("where")) {
            String value = nextFullToken.getValue();
            if (value.equals(";"))
                throw new CoSQLQueryParseError();

            if (nextFullToken.isLiteral()) {
                computeValueStr = computeValueStr.concat("\"").concat(value).concat("\"");
            } else {
                computeValueStr = computeValueStr.concat(nextFullToken.value);
            }
        }

        // tuple condition
        String condition = "";
        while (!((nextFullToken = parseData.nextFullToken()).getValue().equals(";"))) {
            if (nextFullToken.isLiteral()) {
                condition = condition.concat("\"").concat(nextFullToken.getValue()).concat("\"");
            } else {
                condition = condition.concat(nextFullToken.getValue());
            }
        }
        parseData.goPrev();

        TupleCondition tupleCondition = new TupleCondition(condition, tableName);
        CoSQLUpdate updateQuery = new CoSQLUpdate(tableName, columnName, computeValueStr, tupleCondition.getContents());
        parseData.addCommand(updateQuery);
    }


    private void select(ParseData parseData) throws CoSQLQueryParseError {
        ArrayList<String> columnNames = new ArrayList<>();
        String lookAhead;
        while (!((lookAhead = parseData.next()).equalsIgnoreCase("from"))) {
            if (lookAhead.equals(","))
                continue;
            if (lookAhead.equals(";"))
                throw new CoSQLQueryParseError();

            columnNames.add(lookAhead);
        }

        String tableName = tableName(parseData);

        // force WHERE keyword
        lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase("where"))
            throwParseError("Expected keyword WHERE before %s", lookAhead);

        String condition = "";
        LexicalToken nextFullToken;
        while (!((nextFullToken = parseData.nextFullToken()).getValue().equals(";"))) {
            if (nextFullToken.isLiteral()) {
                condition = condition.concat("\"").concat(nextFullToken.getValue()).concat("\"");
            } else {
                condition = condition.concat(nextFullToken.getValue());
            }
        }
        parseData.goPrev();

        TupleCondition tupleCondition = new TupleCondition(condition, tableName);
        CoSQLSelect selectQuery = new CoSQLSelect(
                tableName,
                columnNames,
                tupleCondition
        );
        parseData.addCommand(selectQuery);
    }

    private void delete(ParseData parseData) throws CoSQLQueryParseError {
        // force FROM keyword
        String lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase("from")) {
            throwParseError("Expected keyword FROM before %s", lookAhead);
        }

        String tableName = tableName(parseData);

        // force WHERE keyword
        lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase("where")) {
            throwParseError("Expected keyword WHERE before %s", lookAhead);
        }

        String condition = "";
        LexicalToken nextFullToken;
        while (!((nextFullToken = parseData.nextFullToken()).getValue().equals(";"))) {
            if (nextFullToken.isLiteral()) {
                condition = condition.concat("\"").concat(nextFullToken.getValue()).concat("\"");
            } else {
                condition = condition.concat(nextFullToken.getValue());
            }
        }
        parseData.goPrev();

        TupleCondition tupleCondition = new TupleCondition(condition, tableName);

        CoSQLDelete deleteQuery = new CoSQLDelete(tableName, tupleCondition.getContents());
        parseData.addCommand(deleteQuery);
    }

    // this method is for debugging only ..
    private void print(ParseData parseData) throws CoSQLQueryParseError {
        // get tableName
        String tableName = tableName(parseData);

        CoSQLPrintTable printTableQuery = new CoSQLPrintTable(tableName);
        parseData.addCommand(printTableQuery);
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

    private String indexName(ParseData parseData) throws CoSQLQueryParseError {

        // get token
        String indexName = parseData.next();

        // match valid
        if (!indexName.matches(REGEX_INDEX_NAME)) {
            String error = String.format("Illegal table name: '%s'", indexName);
            throw new CoSQLQueryParseError(error);
        }

        return indexName;
    }

    private void create(ParseData parseData) throws EndOfBufferException, CoSQLQueryParseError, CoSQLQueryExecutionError {

        String lookAhead = parseData.next();

        if (lookAhead.equalsIgnoreCase("database")) {
            createDatabase(parseData);
        } else if (lookAhead.equalsIgnoreCase("table")) {
            createTable(parseData);
        } else if (lookAhead.equalsIgnoreCase("index")) {
            createIndex(parseData);
        } else {
            String error = String.format("Unexpected \'%s\' after CREATE.", lookAhead);
            throw new CoSQLQueryParseError(error);
        }

    }

    private void createIndex(ParseData parseData) throws CoSQLQueryParseError {

        String indexName = indexName(parseData);

        // force ON
        match("on", "Expected keyword ON before %s");

        String tableName = tableName(parseData);

        // force '('
        match("(");

        // extract column name
        String columnName = columnName(parseData);

        // force ')'
        match(")");

        // create command
        CoSQLCreateIndex createIndexQuery = new CoSQLCreateIndex(indexName, tableName, columnName);
        parseData.addCommand(createIndexQuery);
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

        // force '('
        match("(");

        ArrayList<Column> columns = new ArrayList<>();

        Column first = tableColumn(parseData); // TODO proper error report while throwing
        columns.add(first);

        while (true) {
            String lookAhead = parseData.next();
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

        String pkColumn, fkColumn, action1, action2, tableReference;
        ArrayList<String[]> FKarrays = new ArrayList<>();
        int i = 0 , j =0;
        boolean isFK = true;
        String[] arr2 = new String[4];
        String lookAhead = parseData.next();

        if (lookAhead.equals(";")) {

            pkColumn = null;
            //for (String s : arr2) {
            //  s = null;}
            //FKarrays.add(arr2);
            parseData.goPrev();
        } else {

            /**** PRIMARY KEY ****/
            if (lookAhead.equalsIgnoreCase("primary")) {
                j++;
                parseData.next();
                pkColumn = columnName(parseData);
                lookAhead = parseData.next();
            } else pkColumn = null;

            if (lookAhead.equals(";"))
                parseData.goPrev();
            else {
                while (isFK) {
                    String[] arr1 = new String[4];

                    /**** FOREIGN KEY ****/
                    if (lookAhead.equalsIgnoreCase("foreign")) {
                        parseData.next();
                        fkColumn = columnName(parseData);
                        lookAhead = parseData.next();
                    } else fkColumn = null;
                    arr1[0] = fkColumn;

                    if (lookAhead.equals(";"))
                        parseData.goPrev();
                    else {

                        /**** REFERENCE TABLE ****/
                        if (lookAhead.equalsIgnoreCase("references")) {
                            tableReference = tableName(parseData);
                            lookAhead = parseData.next();
                        } else tableReference = null;
                        arr1[1] = tableReference;

                        if (lookAhead.equals(";"))
                            parseData.goPrev();
                        else {

                            /**** on delete ****/
                            if (lookAhead.equalsIgnoreCase("on") && parseData.next().equalsIgnoreCase("delete")) {
                                j++;
                                if (parseData.next().equalsIgnoreCase("cascade"))
                                    action1 = "cascade";
                                else
                                    action1 = "restrict";
                                lookAhead = parseData.next();
                            } else action1 = null;
                            arr1[2] = action1;

                            if (lookAhead.equals(";"))
                                parseData.goPrev();
                            else {

                                /**** on update ****/
                                if (lookAhead.equalsIgnoreCase("on") && parseData.next().equalsIgnoreCase("update")) {
                                    if (parseData.next().equalsIgnoreCase("cascade"))
                                        action2 = "cascade";
                                    else
                                        action2 = "restrict";
                                    lookAhead = parseData.next();
                                } else action2 = null;

                                arr1[3] = action2;
                                FKarrays.add(arr1);
                                if (lookAhead.equals(";")) {
                                    parseData.goPrev();
                                    isFK = false;
                                    break;
                                } else
                                    i++;
                            }
                        }
                    }
                }
            }
        }

//        for (String[] s : FKarrays) {
//            for (String s2: s) {
//                System.out.println(s2);
//
//            }
//        }
        CoSQLCommand command = new CoSQLCreateTable(name, columns, pkColumn, FKarrays);
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

    private void end(ParseData parseData) throws CoSQLError {

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

    private void match(String s) throws CoSQLQueryParseError {
        String lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase(s)) {
            String error = String.format("Expected '%s' before %s", s, lookAhead);
            throw new CoSQLQueryParseError(error);
        }
    }

    private void match(String s, String errorMessage) throws CoSQLQueryParseError {
        String lookAhead = parseData.next();
        if (!lookAhead.equalsIgnoreCase(s)) {
            String error = String.format(errorMessage, lookAhead);
            throw new CoSQLQueryParseError(error);
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

        void goPrev() {
            if (next != 0)
                next--;
        }

        void goToLast() {
            next = tokens.size() - 1;
        }

        LexicalToken peek() {
            return tokens.get(next);
        }

        boolean hasNext() {
            return next < tokens.size();
        }

        String rest() {
            StringBuilder sb = new StringBuilder();
            for (int i = next; i < tokens.size(); i++) {
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

        void batchRun() throws CoSQLError {
            for (CoSQLCommand command : commands) {
                command.execute(); // TODO batch run might get messed if something goes wrong in the middle, proper revert system needed
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
            for (String string : bySpace) {
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

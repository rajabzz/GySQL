package dbms.parser;

import dbms.engine.DatabaseCore;
import dbms.engine.Table;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rajabzz on 11/12/15.
 */
public class TupleCondition {
    private static final String REGEX_TRUE = "TRUE";
    private static final String REGEX_FALSE = "FALSE";
    private static final String REGEX_EQUAL = "(.*)=(.*)";
    private static final String REGEX_LESS_THAN = "(.*)<(.*)";
    private static final String REGEX_LESS_THAN_OR_EQUAL = "(.*)<=(.*)";
    private static final String REGEX_GREATER = "(.*)>(.*)";
    private static final String REGEX_GREATER_OR_EQUAL = "(.*)>=(.*)";
    private static final String REGEX_AND = "\\((.*)\\)AND\\((.*)\\)";
    private static final String REGEX_OR = "\\((.*)\\)OR\\((.*)\\)";
    private static final String REGEX_NOT = "NOT(.*)";

    private static final int INDEX_NOT = 0;
    private static final int INDEX_OR = 1;
    private static final int INDEX_AND = 2;
    private static final int INDEX_GREATER_OR_EQUAL = 3;
    private static final int INDEX_GREATER = 4;
    private static final int INDEX_LESS_THAN_OR_EQUAL = 5;
    private static final int INDEX_LESS_THAN = 6;
    private static final int INDEX_EQUAL = 7;
    private static final int INDEX_FALSE = 8;
    private static final int INDEX_TRUE = 9;

    private static final Pattern[] patterns = {
            Pattern.compile(REGEX_NOT),
            Pattern.compile(REGEX_OR),
            Pattern.compile(REGEX_AND),
            Pattern.compile(REGEX_GREATER_OR_EQUAL),
            Pattern.compile(REGEX_GREATER),
            Pattern.compile(REGEX_LESS_THAN_OR_EQUAL),
            Pattern.compile(REGEX_LESS_THAN),
            Pattern.compile(REGEX_EQUAL),
            Pattern.compile(REGEX_FALSE),
            Pattern.compile(REGEX_TRUE)
    };

    private ArrayList<Table.Row> contents;
    private Table table;
    private Matcher matcher;

    public TupleCondition(String rawStr, String tableName) {
        try {
            table = DatabaseCore.getTable(tableName);
            int patternIndex = getPatternIndex(rawStr);
            ArrayList<Table.Row> secondContents;
            switch (patternIndex) {
                case INDEX_NOT:
                    contents = table.getContents();
                    secondContents = (new TupleCondition(matcher.group(1), tableName)).getContents();
                    for (Table.Row row: secondContents) {
                        if (contents.contains(row)) {
                            contents.remove(row);
                        }
                    }
                    break;

                case INDEX_AND:
                    contents = (new TupleCondition(matcher.group(1), tableName)).getContents();
                    secondContents = (new TupleCondition(matcher.group(2), tableName)).getContents();
                    for (Table.Row row: secondContents) {
                        if (contents.contains(row)) {
                            contents.remove(row);
                        }
                    }
                    break;

                case INDEX_OR:
                    contents = (new TupleCondition(matcher.group(1), tableName).getContents());
                    secondContents = (new TupleCondition(matcher.group(2), tableName)).getContents();
                    for (Table.Row row: secondContents) {
                        contents.add(row);
                    }
                    break;

                case INDEX_EQUAL:
                    contents = DatabaseCore.getContents(
                            tableName,
                            matcher.group(1),
                            matcher.group(2),
                            DatabaseCore.COMPARISON_TYPE_EQUAL
                    );
                    break;

                case INDEX_GREATER_OR_EQUAL:
                    contents = DatabaseCore.getContents(
                            tableName,
                            matcher.group(1),
                            matcher.group(2),
                            DatabaseCore.COMPARISON_TYPE_GREATER_OR_EQUAL
                    );
                    break;

                case INDEX_GREATER:
                    contents = DatabaseCore.getContents(
                            tableName,
                            matcher.group(1),
                            matcher.group(2),
                            DatabaseCore.COMPARISON_TYPE_GREATER
                    );
                    break;

                case INDEX_LESS_THAN:
                    contents = DatabaseCore.getContents(
                            tableName,
                            matcher.group(1),
                            matcher.group(2),
                            DatabaseCore.COMPARISON_TYPE_LESS_THAN
                    );
                    break;

                case INDEX_LESS_THAN_OR_EQUAL:
                    contents = DatabaseCore.getContents(
                            tableName,
                            matcher.group(1),
                            matcher.group(2),
                            DatabaseCore.COMPARISON_TYPE_LESS_THAN_OR_EQUAL
                    );
                    break;

                case INDEX_TRUE:
                    contents = table.getContents();
                    break;

                case INDEX_FALSE:
                    contents = new ArrayList<>();
                    break;

                default:
                    System.err.println("mage mishe ?! :|");
            }
        } catch (CoSQLQueryParseError | CoSQLQueryExecutionError coSQLQueryParseError) {
            coSQLQueryParseError.printStackTrace();
        }
    }

    private int getPatternIndex(String rawStr) throws CoSQLQueryParseError {
        for (int i = 0; i < patterns.length; i++) {
            if ((matcher = patterns[i].matcher(rawStr)).matches())
                return i;
        }
        throw new CoSQLQueryParseError("Doesn't match to any TUPLE_CONDITION patterns!");
    }

    public ArrayList<Table.Row> getContents() {
        return contents;
    }
}

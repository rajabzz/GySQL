package dbms.parser;

import dbms.engine.Table;
import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.util.GroupHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by blackvvine on 1/28/16.
 */
public class HavingCondition {

    private class NumRowTuple {
        double num = 0;
        Table.Row row;
    }

    private static final String REGEX_GREATER_OR_EQUAL = "(.*)>=(.*)";
    private static final String REGEX_LESS_THAN_OR_EQUAL = "(.*)<=(.*)";
    private static final String REGEX_GREATER = "(.*)>(.*)";
    private static final String REGEX_LESS_THAN = "(.*)<(.*)";
    private static final String REGEX_EQUAL = "(.*)=(.*)";
    private static final String REGEX_MAX = "MAX\\((.*)\\)";
    private static final String REGEX_MIN = "MIN\\((.*)\\)";
    private static final String REGEX_AVG = "AVG\\((.*)\\)";
    private static final String REGEX_SUM = "SUM\\((.*)\\)";

    private static final int INDEX_MAX = 0;
    private static final int INDEX_MIN = 1;
    private static final int INDEX_AVG = 2;
    private static final int INDEX_SUM = 3;



    private static final int INDEX_GREATER_OR_EQUAL = 0;
    private static final int INDEX_LESS_THAN_OR_EQUAL = 1;
    private static final int INDEX_GREATER = 2;
    private static final int INDEX_LESS_THAN = 3;
    private static final int INDEX_EQUAL = 4;

    private static final Pattern[] firstPatterns = {
            Pattern.compile(REGEX_GREATER_OR_EQUAL),
            Pattern.compile(REGEX_LESS_THAN_OR_EQUAL),
            Pattern.compile(REGEX_GREATER),
            Pattern.compile(REGEX_LESS_THAN),
            Pattern.compile(REGEX_EQUAL),
    };

    private static final Pattern[] secPatterns = {
            Pattern.compile(REGEX_MAX),
            Pattern.compile(REGEX_MIN),
            Pattern.compile(REGEX_AVG),
            Pattern.compile(REGEX_SUM),
    };

    private ArrayList<Table.Row> contents;
    private Matcher matcher;
    private double num;


    public enum ThetaOperator {

        EQ("="), GE(">="), LE("<="), GT(">"), LT("<");

        private final String text;

        ThetaOperator(String text) {
            this.text = text;
        }

        static ThetaOperator fromText(String text) {
            for (ThetaOperator m: ThetaOperator.values()) {
                if (m.text.equalsIgnoreCase(text)) {
                    return m;
                }
            }
            return null;
        }

    }

    public HavingCondition(String rawStr, Table selectedTable, Table sourceTable,
                           Map<Table.Row, Table.Row> sigmaMap, GroupHashMap<List<Object>, Table.Row> map) {


        contents = new ArrayList<>();
        try {
            int patternIndex = getPatternIndexFirst(rawStr);

            switch (patternIndex) {
                case INDEX_GREATER_OR_EQUAL: {
                    String left = matcher.group(1);
                    String right = matcher.group(2);

                    ArrayList<NumRowTuple> leftContents = parse(left, selectedTable, sourceTable, sigmaMap, map);
                    ArrayList<NumRowTuple> rightContents = parse(right, selectedTable, sourceTable, sigmaMap, map);

                    if (rightContents == null) {
                        double num = compute(right);
                        for (NumRowTuple nr: leftContents) {
                            if (nr.num >= num) {
                                contents.add(nr.row);
                            }
                        }
                    } else if (leftContents == null) {
                        double num = compute(left);
                        for (NumRowTuple nr: rightContents) {
                            if (num >= nr.num) {
                                contents.add(nr.row);
                            }
                        }
                    } else {
                        System.err.println(":| :| :| :|");
                    }

                    break;
                }


                case INDEX_GREATER: {
                    String left = matcher.group(1);
                    String right = matcher.group(2);

                    ArrayList<NumRowTuple> leftContents = parse(left, selectedTable, sourceTable, sigmaMap, map);
                    ArrayList<NumRowTuple> rightContents = parse(right, selectedTable, sourceTable, sigmaMap, map);

                    if (rightContents == null) {
                        double num = compute(right);
                        for (NumRowTuple nr: leftContents) {
                            if (nr.num > num) {
                                contents.add(nr.row);
                            }
                        }
                    } else if (leftContents == null) {
                        double num = compute(left);
                        for (NumRowTuple nr: rightContents) {
                            if (num > nr.num) {
                                contents.add(nr.row);
                            }
                        }
                    } else {
                        System.err.println(":| :| :| :|");
                    }

                    break;
                }

                case INDEX_LESS_THAN_OR_EQUAL: {
                    String left = matcher.group(1);
                    String right = matcher.group(2);

                    ArrayList<NumRowTuple> leftContents = parse(left, selectedTable, sourceTable, sigmaMap, map);
                    ArrayList<NumRowTuple> rightContents = parse(right, selectedTable, sourceTable, sigmaMap, map);

                    if (rightContents == null) {
                        double num = compute(right);
                        for (NumRowTuple nr : leftContents) {
                            if (nr.num <= num) {
                                contents.add(nr.row);
                            }
                        }
                    } else if (leftContents == null) {
                        double num = compute(left);
                        for (NumRowTuple nr : rightContents) {
                            if (num <= nr.num) {
                                contents.add(nr.row);
                            }
                        }
                    } else {
                        System.err.println(":| :| :| :|");
                    }
                    break;
                }

                case INDEX_LESS_THAN: {
                    String left = matcher.group(1);
                    String right = matcher.group(2);

                    ArrayList<NumRowTuple> leftContents = parse(left, selectedTable, sourceTable, sigmaMap, map);
                    ArrayList<NumRowTuple> rightContents = parse(right, selectedTable, sourceTable, sigmaMap, map);

                    if (rightContents == null) {
                        double num = compute(right);
                        for (NumRowTuple nr : leftContents) {
                            if (nr.num < num) {
                                contents.add(nr.row);
                            }
                        }
                    } else if (leftContents == null) {
                        double num = compute(left);
                        for (NumRowTuple nr : rightContents) {
                            if (num < nr.num) {
                                contents.add(nr.row);
                            }
                        }
                    } else {
                        System.err.println(":| :| :| :|");
                    }
                    break;
                }

                case INDEX_EQUAL: {
                    String left = matcher.group(1);
                    String right = matcher.group(2);

                    ArrayList<NumRowTuple> leftContents = parse(left, selectedTable, sourceTable, sigmaMap, map);
                    ArrayList<NumRowTuple> rightContents = parse(right, selectedTable, sourceTable, sigmaMap, map);

                    if (rightContents == null) {
                        double num = compute(right);
                        for (NumRowTuple nr : leftContents) {
                            if (nr.num == num) {
                                contents.add(nr.row);
                            }
                        }
                    } else if (leftContents == null) {
                        double num = compute(left);
                        for (NumRowTuple nr : rightContents) {
                            if (num == nr.num) {
                                contents.add(nr.row);
                            }
                        }
                    } else {
                        System.err.println(":| :| :| :|");
                    }
                    break;
                }

                default:
                    System.err.println("mage mishe ?! :|");
            }
        } catch (CoSQLQueryParseError coSQLQueryParseError) {
            coSQLQueryParseError.printStackTrace();
        }

    }


    private ArrayList<NumRowTuple> parse(String rawStr, Table selectedTable, Table sourceTable,
                                         Map<Table.Row, Table.Row> sigmaMap, GroupHashMap<List<Object>, Table.Row> map) {

        try {
            int patternIndex = getPatternIndexSec(rawStr);

            switch (patternIndex) {
                case INDEX_MAX: {
                    ArrayList<NumRowTuple> contentNum = new ArrayList<>();
                    int columnIndex = sourceTable.getColumnIndex(matcher.group(1));
                    for (List<Object> uniqueVal : map.keySet()) {
                        long max = 0;
                        Table.Row maxRow = null;
                        for (Table.Row row : map.get(uniqueVal)) {
                            Table.Row originalRow = sigmaMap.get(row);
                            long l = (long) originalRow.getValueAt(columnIndex);
                            if (l > max) {
                                max = l;
                                maxRow = row;
                            }
                        }
                        NumRowTuple nr = new NumRowTuple();
                        nr.num = max;
                        nr.row = maxRow;

                        contentNum.add(nr);
                    }
                    return contentNum;
                }
                case INDEX_MIN: {
                    ArrayList<NumRowTuple> contentNum = new ArrayList<>();
                    int columnIndex = sourceTable.getColumnIndex(matcher.group(1));
                    for (List<Object> uniqueVal : map.keySet()) {
                        long min = Long.MAX_VALUE;
                        Table.Row minRow = null;
                        for (Table.Row row : map.get(uniqueVal)) {
                            Table.Row originalRow = sigmaMap.get(row);
                            long l = (long) originalRow.getValueAt(columnIndex);
                            if (l < min) {
                                min = l;
                                minRow = row;
                            }
                        }

                        NumRowTuple nr = new NumRowTuple();
                        nr.num = min;
                        nr.row = minRow;

                        contentNum.add(nr);
                    }
                    return contentNum;
                }
                case INDEX_AVG: {
                    ArrayList<NumRowTuple> contentNum = new ArrayList<>();
                    int columnIndex = sourceTable.getColumnIndex(matcher.group(1));
                    for (List<Object> uniqueVal : map.keySet()) {
                        long sum = 0;
                        int counter = 0;
                        Table.Row r = null;
                        for (Table.Row row : map.get(uniqueVal)) {
                            Table.Row originalRow = sigmaMap.get(row);
                            long l = (long) originalRow.getValueAt(columnIndex);
                            sum += l;
                            counter++;
                            r = row;
                        }

                        NumRowTuple nr = new NumRowTuple();
                        nr.num = sum / counter;
                        nr.row = r;

                        contentNum.add(nr);
                    }
                    return contentNum;
                }
                case INDEX_SUM: {
                    ArrayList<NumRowTuple> contentNum = new ArrayList<>();

                    int columnIndex = sourceTable.getColumnIndex(matcher.group(1));
                    for (List<Object> uniqueVal : map.keySet()) {
                        long sum = 0;
                        Table.Row r = null;
                        for (Table.Row row : map.get(uniqueVal)) {
                            Table.Row originalRow = sigmaMap.get(row);
                            long l = (long) originalRow.getValueAt(columnIndex);
                            sum += l;
                            r = row;
                        }

                        NumRowTuple nr = new NumRowTuple();
                        nr.num = sum;
                        nr.row = r;

                        contentNum.add(nr);
                    }
                    return contentNum;
                }

                default: {
                }
            }

        } catch (CoSQLError coSQLError) {
            coSQLError.printStackTrace();
        }

        return null;
    }

    private double compute(String rawInput) throws CoSQLQueryParseError {
        return (long) ValueComputer.computeConstant(rawInput);
    }


    GroupByData.Method method;
    String columnName;
    ThetaOperator theta;
    LexicalToken value;

    public HavingCondition(GroupByData.Method method, String columnName, ThetaOperator theta, LexicalToken value) {
        this.method = method;
        this.columnName = columnName;
        this.theta = theta;
        this.value = value;
    }

    public GroupByData.Method getMethod() {
        return method;
    }

    public String getColumnName() {
        return columnName;
    }

    public ThetaOperator getTheta() {
        return theta;
    }

    public LexicalToken getValue() {
        return value;
    }


    private int getPatternIndexFirst(String rawStr) throws CoSQLQueryParseError {
        for (int i = 0; i < firstPatterns.length; i++) {
            if ((matcher = firstPatterns[i].matcher(rawStr)).matches())
                return i;
        }
        throw new CoSQLQueryParseError("Doesn't match to any HAVING_CONDITION patterns!");
    }

    private int getPatternIndexSec(String rawStr) throws CoSQLQueryParseError {
        for (int i = 0; i < secPatterns.length; i++) {
            if ((matcher = secPatterns[i].matcher(rawStr)).matches())
                return i;
        }

        return -1;
    }

    public ArrayList<Table.Row> getContents() {
        return contents;
    }

}

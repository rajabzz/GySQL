package dbms.parser;

import dbms.exceptions.CoSQLQueryParseError;
import jdk.internal.dynalink.linker.TypeBasedGuardingDynamicLinker;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rajabzz on 11/12/15.
 */
public class TupleCondition {
    private static final String REGEX_TRUE = "\bTRUE\b";
    private static final String REGEX_FALSE = "\bFALSE\b";
    private static final String REGEX_EQUAL = "(.*)=(.*)";
    private static final String REGEX_LESS_THAN = "(.*)<(.*)";
    private static final String REGEX_LESS_THAN_OR_EQUAL = "(.*)<=(.*)";
    private static final String REGEX_GREATER = "(.*)>(.*)";
    private static final String REGEX_GREATER_OR_EQUAL = "(.*)>=(.*)";
    private static final String REGEX_AND = "\\((.*)\\) AND \\((.*)\\)";
    private static final String REGEX_OR = "\\((.*)\\) OR \\((.*)\\)";
    private static final String REGEX_NOT = "NOT (.*)";

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

    private Matcher matcher;

    public TupleCondition(String rawStr) {
        try {
            ComputeValue valueComputer = new ComputeValue();
            int index = getPatternIndex(rawStr);
            switch (index) {
                case INDEX_NOT:
                    break;

                case INDEX_AND:
                    break;

                case INDEX_OR:
                    break;

                case INDEX_EQUAL:
                    break;

                case INDEX_GREATER_OR_EQUAL:
                    break;

                case INDEX_GREATER:
                    break;

                case INDEX_LESS_THAN:
                    break;

                case INDEX_LESS_THAN_OR_EQUAL:
                    break;

                case INDEX_TRUE:
                    break;

                case INDEX_FALSE:
                    break;

                default:
                    System.err.println("mage mishe ?! :|");
            }
        } catch (CoSQLQueryParseError coSQLQueryParseError) {
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

    public static void main(String[] args) {
        TupleCondition tupleCondition = new TupleCondition("(ID=92106431) AND (FNAME=\"SAND\")");
    }
}

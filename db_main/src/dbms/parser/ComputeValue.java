package dbms.parser;

import dbms.exceptions.CoSQLQueryParseError;
import java.util.StringTokenizer;

public class ComputeValue {

    /**
     * Author: Fucking Molla
     */
    static final String delimiters = "+-*/";

    public LexicalToken compute(String rawInput) throws CoSQLQueryParseError{

        LexicalToken dummy = new LexicalToken("", true);
        LexicalToken result = dummy;
        LexicalToken first  = dummy;
        LexicalToken second = dummy;

//        rawInput = removeFields(rawInput);
        int index = rawInput.length() - 1;

        while (index >= 0) {
            if (delimiters.contains("" + rawInput.charAt(index))) {
                first = compute( rawInput.substring(0, index) );
                second = singleTokenOperate( rawInput.substring(index + 1) );
                break;
            }
            index--;
        }
        if (index == -1) {
            result = singleTokenOperate(rawInput);
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

    private LexicalToken singleTokenOperate(String rawToken) throws CoSQLQueryParseError{
        // rawToken is INT or String between 's or "s
        if (rawToken.contains("\'") || rawToken.contains("\"")) {
            return new LexicalToken(rawToken.substring(1, rawToken.length()-1), true);
        }
        return new LexicalToken(rawToken, false);
    }

//    private String removeFields(String rawInput /* , Table table, index or record */) {
//
//        String result = "";
//        StringTokenizer tokenizer = new StringTokenizer(rawInput, delimiters, true);
//
//        while (tokenizer.hasMoreTokens()) {
//            String string = tokenizer.nextToken();
//
//            if (delimiters.contains(string) || string.contains("\'") || string.contains("\""))
//                result += string;
//            else {
//                try {
//                    int num = Integer.parseInt(string);
//                    result += string;
//                } catch (NumberFormatException e) {
//                    // it is a field
//                    // TODO replace it with actual value --> An INT or VARCHAR in quotations
//                    // TODO how to find out the record with which should be replaced the fields
//                }
//            }
//        }
//        return result;
//    }
}
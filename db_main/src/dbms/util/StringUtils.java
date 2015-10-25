package dbms.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.parser.LexicalToken;

/**
 * Created by blackvvine on 10/25/15.
 */
public class StringUtils {

    private static final String[] IGNORE = new String[]{"\t", "\n", " "};

    public static List<LexicalToken> tokenizeQuery(String command) throws CoSQLQueryParseError {

        List<LexicalToken> res = new ArrayList<LexicalToken>();

        // use Java tokenizer for raw tokenize
        StringTokenizer tokenizer = new StringTokenizer(command, " \n\t\r\"\'();,=", true);

        boolean literalMode = false;
        String buffer = null;

        // iterate through raw tokens
        main_loop:
        while (tokenizer.hasMoreTokens()) {

            String tk = tokenizer.nextToken();

            if (tk.equals("\"") || tk.equals("\'")) {

                if (literalMode) {
                    literalMode = false;
                    res.add(new LexicalToken(buffer, true));
                    buffer = null;
                } else {
                    literalMode = true;
                    buffer = "";
                }

                continue;
            }

            if (literalMode) {

                buffer += tk;

            } else {

                for (String ws: IGNORE) {
                    if (ws.equals(tk)) {
                        continue main_loop;
                    }
                }

                res.add(new LexicalToken(tk, false));
            }

        }

        if (literalMode) {
            throw new CoSQLQueryParseError("Unexpected end of query (literal not finished)");
        }

        return res;
    }

}

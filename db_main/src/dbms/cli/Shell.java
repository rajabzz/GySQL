package dbms.cli;

import java.util.HashMap;
import java.util.Scanner;

import dbms.UserInterface;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;
import dbms.exceptions.EndOfSessionException;
import dbms.parser.QueryParser;

import static dbms.DatabaseBible.*;

/**
 * Created by blackvvine on 10/25/15.
 */
public class Shell implements UserInterface {

    static HashMap<Integer, String> messageMap;

    private static void init() {
        messageMap = new HashMap<Integer, String>();

        if (JUDGE_MODE) {
            messageMap.put(EVENT_CREATE_DATABASE, "DATABASE CREATED");
        }
    }

    static {
        init();
    }

    @Override
    public void event(int code) {

    }

    private void exec() {
        // standard input hook
        Scanner stdin = new Scanner(System.in);

        // instantiate query parser
        QueryParser queryParser = new QueryParser(this);

        while (true) {

            // TODO  back slash escape
            String line = stdin.nextLine();

            try {

                queryParser.parseAndRun(line);

            } catch (EndOfSessionException e) {

                e.printStackTrace();
                break;

            } catch (CoSQLQueryParseError coSQLQueryParseError) {

                coSQLQueryParseError.printStackTrace();
                System.out.println(coSQLQueryParseError);

            } catch (CoSQLQueryExecutionError coSQLQueryExecutionError) {
                coSQLQueryExecutionError.printStackTrace();
                System.out.println(coSQLQueryExecutionError);
            }

        }
    }

    public static void main(String... args) {

        Shell shell = new Shell();
    }
}

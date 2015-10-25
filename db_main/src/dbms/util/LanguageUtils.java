package dbms.util;

import dbms.exceptions.CoSQLError;
import dbms.exceptions.CoSQLQueryExecutionError;
import dbms.exceptions.CoSQLQueryParseError;

/**
 * Created by blackvvine on 10/26/15.
 */
public class LanguageUtils {

    public static void throwCoSQLError(Class<? extends CoSQLError> type, String format, Object... args) throws CoSQLError {
        try {
            String message = String.format(format, args);
            CoSQLError res = (CoSQLError) type.getConstructor(new Class[]{String.class}).newInstance(message);
            throw res;
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void throwParseError(String format, Object... args) throws CoSQLQueryParseError {
        throw new CoSQLQueryParseError(String.format(format, args));
    }

    public static void throwExecError(String format, Object... args) throws CoSQLQueryExecutionError {
        throw new CoSQLQueryExecutionError(String.format(format, args));
    }

}

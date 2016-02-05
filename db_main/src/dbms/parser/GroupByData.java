package dbms.parser;

import java.util.List;

/**
 * Created by blackvvine on 1/28/16.
 */
public class GroupByData {

    List<String> columns;
    String rawHavingCondition;

    public GroupByData(List<String> columns, String having) {
        this.columns = columns;
        this.rawHavingCondition = having;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getRawHavingCondition() {
        return rawHavingCondition;
    }

    public enum Method {

        MIN("MIN"), MAX("MAX"), SUM("SUM"), AVG("AVG");

        private String text;

        Method(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public static Method fromText(String text) {
            for (Method m: Method.values()) {
                if (m.text.equalsIgnoreCase(text)) {
                    return m;
                }
            }
            return null;
        }

        public static boolean isAggregateFunction(String text) {
            for (Method m: Method.values()) {
                if (m.text.equalsIgnoreCase(text))
                    return true;
            }
            return false;
        }

    }
}

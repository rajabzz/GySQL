package dbms.parser;

/**
 * Created by blackvvine on 1/28/16.
 */
public class HavingCondition {

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

}

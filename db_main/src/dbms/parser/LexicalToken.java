package dbms.parser;

/**
 * Created by blackvvine on 10/25/15.
 */
public class LexicalToken {

    String value;
    boolean literal;

    public LexicalToken(String value, boolean literal) {
        this.value = value;
        this.literal = literal;
    }

    public String getValue() {
        return value;
    }

    public boolean isLiteral() {
        return literal;
    }

    @Override
    public String toString() {
        return String.format("<%s %s>", value, literal ? "L" : "NL");
    }
}

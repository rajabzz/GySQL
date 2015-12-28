package dbms.test;

import dbms.parser.LexicalToken;
import dbms.util.StringUtils;

import java.util.List;
import java.util.StringTokenizer;

public class Test {
    public static void main(String... args) {
        try {
            List<LexicalToken> tokens = StringUtils.tokenizeQuery("sddffaf.gfdd,ddfdf.ds,s,ds.");
            for (LexicalToken m : tokens) {
                System.out.println(m.getValue());
            }
        }catch(Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
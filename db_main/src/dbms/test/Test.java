package dbms.test;

import dbms.parser.LexicalToken;
import dbms.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Test {
    public static void main(String... args) {
        ArrayList<String> a = new ArrayList<>();

        System.out.println(a.isEmpty());
        for (String r: a) {
            System.out.println(r);
        }
    }
}
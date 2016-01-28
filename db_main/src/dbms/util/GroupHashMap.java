package dbms.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A convenience class for "Multi Value Table"
 * the values are the table are a list of K objects
 * which get created upon first add with a certain key
 */
public class GroupHashMap<T, K> {

    Map<T, List<K>> table = new HashMap<>();

    public void add(T key, K value) {

        List<K> valList = table.get(key);

        if (valList != null) {
            valList.add(value);
        } else {
            valList = new ArrayList<>();
            valList.add(value);
            table.put(key, valList);
        }

    }

}

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
public class GroupHashMap<T, K> extends HashMap<T, List<K>> {

    public void add(T key, K value) {

        List<K> valList = this.get(key);

        if (valList != null) {
            valList.add(value);
        } else {
            valList = new ArrayList<>();
            valList.add(value);
            this.put(key, valList);
        }

    }

}

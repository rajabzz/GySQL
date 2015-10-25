package dbms.engine;

import java.io.Serializable;

/**
 * Created by blackvvine on 10/25/15.
 */
public class Authority implements Serializable {

    enum AuthMethod {
        NONE, PASSWORD
    };

    String user;

    AuthMethod authMethod;

    String passwordSha;

}

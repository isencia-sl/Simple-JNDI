package issues;

import org.junit.After;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.util.Hashtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author ognivo777@gmail.com
 * @since 2023-03-18
 */
public class Issue31Test {
    private InitialContext initialContext;

    @After
    public void tearDown() throws NamingException {
        System.clearProperty("java.naming.factory.initial");
        System.clearProperty("org.osjava.sj.root");
        System.clearProperty("org.osjava.sj.delimiter");
        System.clearProperty("org.osjava.sj.space");
        System.clearProperty("org.osjava.sj.jndi.shared");
        if (initialContext != null) {
            initialContext.close();
            initialContext = null;
        }
    }

    /**
     * <p>The context should be shared. But up to 0.18.1 it was not, because org.osjava.sj.* properties were not considered in {@link org.osjava.sj.SimpleJndiContextFactory#getInitialContext(Hashtable)}.
     * </p><p>
     * System properties are now handled as soon as possible.
     * </p>
     */
    @Test
    public void issue31() throws NamingException {

        System.setProperty("java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory");
        System.setProperty("org.osjava.sj.root", "src/test/resources/roots/commons/configuration/slashDelimiter");
        System.setProperty("org.osjava.sj.delimiter", "/");
        System.setProperty("org.osjava.sj.space", "java:comp/env");
        System.setProperty("org.osjava.sj.jndi.shared", "true");
        try {
            initialContext = new InitialContext();
        } catch(NamingException ne) {
            ne.printStackTrace();
        }

        Context envContext = (Context) initialContext.lookup("java:comp/env/");
    }

}

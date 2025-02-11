/*
 * Copyright (c) 2003-2005, Henri Yandell
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the 
 * following conditions are met:
 * 
 * + Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer.
 * 
 * + Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution.
 * 
 * + Neither the name of Simple-JNDI nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software 
 *   without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.osjava.sj.loader;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osjava.datasource.SJDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.junit.Assert.*;

public class FileBasedJndiLoaderTest {

    private Context ctxt;
    private FileBasedJndiLoader loader;

    @Before
    public void setUp() {

        /* The default is 'flat', which isn't hierarchial and not what I want. */
        /* Separator is required for non-flat */

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.osjava.sj.MemoryContextFactory");
        env.put("jndi.syntax.direction", "left_to_right");
        env.put("jndi.syntax.separator", "/");
        env.put(JndiLoader.DELIMITER, "/");

        /* For Directory-Naming
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        env.put(Context.URL_PKG_PREFIXES, "org.apache.naming");
        env.put("jndi.syntax.direction", "left_to_right");
        env.put("jndi.syntax.separator", "/");
        */


        loader = new FileBasedJndiLoader(env);
        
        try {
            ctxt = new InitialContext(env);
        } catch(NamingException ne) {
            ne.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        this.ctxt = null;
    }

    @Test
    public void testProperties() {
        try {
            Properties props = new Properties();
            props.put("foo", "13");
            props.put("bar/foo", "42");
            props.put("bar/test/foo", "101");
            loader.load( props, ctxt );
            assertEquals( "13", ctxt.lookup("foo") );
            assertEquals( "42", ctxt.lookup("bar/foo") );
            assertEquals( "101", ctxt.lookup("bar/test/foo") );
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testDefaultFile() {
        try {
            File file = new File("src/test/resources/roots/default.properties");
            loader.load( file, ctxt );
            List list = (List) ctxt.lookup("name");
            assertEquals( "Henri", list.get(0) );
            assertEquals( "Fred", list.get(1) );
            assertEquals( "Foo", ctxt.lookup("com.genjava") );
        } catch(IOException ioe) {
            ioe.printStackTrace();
            fail("IOException: "+ioe.getMessage());
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testSubContext() {

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.osjava.sj.MemoryContextFactory");
        env.put("jndi.syntax.direction", "left_to_right");
        env.put("jndi.syntax.separator", "/");
        env.put(JndiLoader.DELIMITER, "/");
        env.put(JndiLoader.FILENAME_TO_CONTEXT, "true");

        /* For Directory-Naming
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        env.put(Context.URL_PKG_PREFIXES, "org.apache.naming");
        env.put("jndi.syntax.direction", "left_to_right");
        env.put("jndi.syntax.separator", "/");
        */

        FileBasedJndiLoader loader = new FileBasedJndiLoader(env);

        try {
            ctxt = new InitialContext(env);
        } catch(NamingException ne) {
            ne.printStackTrace();
        }

        String dsString = "bing::::foofoo::::Boo";
        try {
            File file = new File("src/test/resources/roots/java.properties");
            loader.load( file, ctxt );
            Context subctxt = (Context) ctxt.lookup("java");
            assertEquals( dsString, subctxt.lookup("TestDS").toString() );
            DataSource ds = (DataSource) ctxt.lookup("java/TestDS");
            assertEquals( dsString, ds.toString() );
        } catch(IOException ioe) {
            ioe.printStackTrace();
            fail("IOException: "+ioe.getMessage());
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testTopLevelDataSource() {
        String dsString = "org.gjt.mm.mysql.Driver::::jdbc:mysql://127.0.0.1/tmp::::sa";
        try {
            File file = new File("src/test/resources/roots/TopLevelDS.properties");
            loader.load( file, ctxt );
            DataSource ds = (DataSource) ctxt.lookup("TopLevelDS");
            assertEquals( dsString, ds.toString() );
        } catch(IOException ioe) {
            ioe.printStackTrace();
            fail("IOException: "+ioe.getMessage());
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testBoolean() {
        try {
            Properties props = new Properties();
            props.put("foo", "true");
            props.put("foo/type", "java.lang.Boolean");
            loader.load( props, ctxt );
            assertEquals( new Boolean(true), ctxt.lookup("foo") );
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testSlashSeparatedNamespacedProperty() throws NamingException {
        Properties props = new Properties();
        props.put("my/name", "holger");
        loader.load( props, ctxt );
        String obj = (String) ctxt.lookup("my/name");
        assertEquals("holger", obj);
    }

    @Test
    public void testDate() {
        try {
            Properties props = new Properties();
            props.put("birthday", "2004-10-22");
            props.put("birthday/type", "java.util.Date");
            props.put("birthday/format", "yyyy-MM-dd");

            loader.load( props, ctxt );

            Date d = (Date) ctxt.lookup("birthday");
            Calendar c = Calendar.getInstance();
            c.setTime(d);

            assertEquals( 2004, c.get(Calendar.YEAR) );
            assertEquals( 10 - 1, c.get(Calendar.MONTH) );
            assertEquals( 22, c.get(Calendar.DATE) );
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testConverterPlugin() {
        try {
            Properties props = new Properties();
            props.put("math", "Pi");
            // type is needed here as otherwise it does not know to allow subelements
            props.put("math/type", "magic number");
            props.put("math/converter", "org.osjava.sj.loader.convert.PiConverter");

            loader.load( props, ctxt );

            assertEquals( new Double(Math.PI), ctxt.lookup("math") );
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testBeanConverter() {
        try {
            Properties props = new Properties();
            props.put("bean/type", "org.osjava.sj.loader.TestBean");
            props.put("bean/converter", "org.osjava.sj.loader.convert.BeanConverter");
            props.put("bean/text", "Example");

            loader.load( props, ctxt );

            TestBean testBean = new TestBean();
            testBean.setText("Example");

            assertEquals( testBean, ctxt.lookup("bean") );
        } catch(NamingException ne) {
            ne.printStackTrace();
            fail("NamingException: "+ne.getMessage());
        }
    }

    @Test
    public void testDbcp() throws IOException, NamingException {
        File file = new File("src/test/resources/roots/pooltest");
        loader.load( file, ctxt );
        DataSource ds = (DataSource) ctxt.lookup("TestDS");
        assertNotNull(ds);
        DataSource ds1 = (DataSource) ctxt.lookup("OneDS");
        assertNotNull(ds1);
        DataSource ds2 = (DataSource) ctxt.lookup("TwoDS");
        assertNotNull(ds2);
        DataSource ds3 = (DataSource) ctxt.lookup("ThreeDS");
        assertNotNull(ds3);
        try {
            Connection conn = ds.getConnection();
            fail("No database is hooked up, so this should have failed");
        } catch (SQLException sqle) {
            // expected
        }
    }

    /**
     * For testing legacy {@link SJDataSource} with commons-dbcp.
     */
    public void testPoolLive() throws IOException, NamingException, SQLException {
        Properties props = new Properties();
        props.put("Sybase/type", "javax.sql.DataSource");
        props.put("Sybase/driver", "com.sybase.jdbc3.jdbc.SybDriver");
        props.put("Sybase/pool", "myPool");
        props.put("Sybase/url", "");
        props.put("Sybase/user", "");
        props.put("Sybase/password", "");
        loader.load(props, ctxt);
        DataSource ds = (DataSource) ctxt.lookup("Sybase");
        assertNotNull(ds);

        // creates and accesses the pool
        Connection c = ds.getConnection();
        Statement stmnt = c.createStatement();
        stmnt.execute("select 1");
        ResultSet rs = stmnt.getResultSet();
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        c.close();

        assertEquals(1, result);

        // accesses the already created pool
        c = ds.getConnection();
        stmnt = c.createStatement();
        stmnt.execute("select 1");
        rs = stmnt.getResultSet();
        rs.next();
        result = rs.getInt(1);
        rs.close();
        c.close();

        assertEquals(1, result);
    }

    /**
     * For testing commons-dbcp2's BasicDataSource. For monitoring with JMX you have to set jmxName. See the code. Follow the JMX name syntax: <a href=http://www.oracle.com/us/technologies/java/best-practices-jsp-136021.html>Java Management Extensions (JMX) - Best Practices</a>.
     */
    @Test
    public void testDbcp2BasicDataSource() throws IOException, NamingException, SQLException {

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.osjava.sj.MemoryContextFactory");
        env.put("jndi.syntax.direction", "left_to_right");
        env.put("jndi.syntax.separator", "/");
        env.put(JndiLoader.DELIMITER, "/");
        env.put(Context.OBJECT_FACTORIES, org.apache.commons.dbcp2.BasicDataSourceFactory.class.getName());

        JndiLoader loader = new JndiLoader(env);

        try {
            ctxt = new InitialContext(env);
        } catch(NamingException ne) {
            ne.printStackTrace();
        }

        Properties props = new Properties();
        props.put("Derby/type", "javax.sql.DataSource");
        props.put("Derby/driverClassName", "org.apache.derby.jdbc.ClientDriver");
        props.put("Derby/url", "jdbc:derby://localhost:1528/sandBox");
        props.put("Derby/username", "sandbox");
        props.put("Derby/password", "sandbox");
        // Not working: jmxName is not in BasicDataSourceFactory.ALL_PROPERTIES and so will be not set. You have to set it after creation by calling setJmxName(). See below.
//        props.put("Sybase/jmxName", "org.osjava.sj:type=DS");
        loader.load(props, ctxt);
        BasicDataSource ds = (BasicDataSource) ctxt.lookup("Derby");
        ds.setJmxName("org.osjava.sj:type=DS");

        assertNotNull(ds);

        Connection c = ds.getConnection();
        Statement stmnt = c.createStatement();
        stmnt.execute("select 1 from Person");
        ResultSet rs = stmnt.getResultSet();
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        c.close();

        assertEquals(1, result);
    }

}

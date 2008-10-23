package org.jax.mgi.index.gatherer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.log4j.Logger;
import org.jax.mgi.index.util.SharedDocumentStack;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The AbstractGatherer class is the superclass for all the other gatherers that we use
 * in the indexing software.
 * 
 * It provides the constructor that they all use, as well as an initCap method, a method
 * to execute sql, cleanup method that gracefully closes down all of the jdbc connections,
 * and a run method that must be overrun by any implementing class.
 * 
 * @author mhall
 *
 * @has A single instance of the SharedDocumentStack, which is used to hold the lucene documents
 * that it produces.
 *      A JDBC Connection
 *      A MGI Configuration Object, used to configure this object and any of its children.
 *      
 * @does Defines the list of common services provided to any gatherer, as well as enforcing 
 *      an api on them.
 *
 */

public class AbstractGatherer implements Runnable {

    protected static SharedDocumentStack sis;
    protected static Connection          con;
    protected static Integer             stack_max;
    private static Logger log = 
        Logger.getLogger(AbstractGatherer.class.getName());

    /**
     * Superclass constructor, this pulls in a configuration object 
     * for any implementing class to use, as well as sets up the JDBC 
     * connection.
     * 
     * @param config
     */

    public AbstractGatherer(IndexCfg config) {
        try {
            Class.forName(DB_DRIVER);
            String USER = config.get("MGI_PUBLICUSER");
            String PASSWORD = config.get("MGI_PUBLICPASSWORD");
            stack_max = new Integer(config.get("STACK_MAX"));
            log.debug("INDEX_JDBC_URL: " + config.get("INDEX_JDBC_URL"));
            con = DriverManager.getConnection(
                    config.get("INDEX_JDBC_URL"), USER, PASSWORD);
        } catch (Exception e) {
            log.error(e);
        }
        sis = SharedDocumentStack.getSharedDocumentStack();
    }

    /**
     * Init cap all words in a string passed to this function.  
     * This was being used in enough of the disparate gatherers 
     * that it was moved to the superclass.
     * 
     * This is code taken from another source.
     * 
     * @param in String to be IntiCapped
     * @return String that has been InitCapped
     */

    public static String initCap(String in) {
        if (in == null || in.length() == 0)
            return in;
        
        boolean capitalize = true;
        char[] data = in.toCharArray();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == ' ' || Character.isWhitespace(data[i]))
                capitalize = true;
            else if (capitalize) {
                data[i] = Character.toUpperCase(data[i]);
                capitalize = false;
            } else
                data[i] = Character.toLowerCase(data[i]);
        }
        return new String(data);
    } // initCap

    /**
     * Execute a given SQL Statement.
     * 
     * @param query
     * @return ResultSet
     */

    public static ResultSet execute(String query) {
        ResultSet set;

        try {
            java.sql.Statement stmt = con.createStatement();

            set = stmt.executeQuery(query);
            return set;
        } catch (Exception e) {
            log.error(e);
            return null;
        }
    }

    /**
     * Gracefully close the connections after they are finished 
     * being used by the gatherer.
     */

    public static void cleanup() {
        try {
            con.close();
        } catch (Exception e) {
            log.error(e);
        }
    }
    
    /**
     * This method MUST be overridden by any implementing class, its 
     * where the indexing algorithm is implemented.
     */

    public void run() {

    }

    private String DB_DRIVER = "com.sybase.jdbc3.jdbc.SybDriver"; 
    
}

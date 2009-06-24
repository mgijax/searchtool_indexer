package org.jax.mgi.searchtoolIndexer.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The SQLExecutor class knows how to create connections to both MGD and the
 * SNP Database.  It can also clean up the connections after they are done,
 * and execute queries against a given database.
 * 
 * The class is also smart enough to only open connections when they are needed.
 * If we go to run a new query and a connection hasn't been created yet, we create one.
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * @does Executes SQL Queries against either the SNP or MGD Database.
 *
 */

public class SQLExecutor {

    protected Connection conMGD = null;
    protected Connection conSNP = null;
    private String user;
    private String password;
    private String mgdJDBCUrl;
    private String snpJDBCUrl;
    
    private Date start;
    private Date end;
        
    protected String DB_DRIVER = "com.sybase.jdbc3.jdbc.SybDriver";
    
    protected Logger log = 
        Logger.getLogger(this.getClass().getName());
    
    /**
     * The default constructor sets up all the configuration variables from
     * IndexCfg.
     * 
     * @param config
     */
    
    public SQLExecutor (IndexCfg config) {
        try {
        Class.forName(DB_DRIVER);
        user = config.get("MGI_PUBLICUSER");
        password = config.get("MGI_PUBLICPASSWORD");
        mgdJDBCUrl = config.get("MGD_JDBC_URL");
        snpJDBCUrl = config.get("SNP_JDBC_URL");
        log.info("MGD JDBC URL: " + config.get("MGD_JDBC_URL"));
        log.info("SNP JDBC URL: " + config.get("SNP_JDBC_URL"));
        }
        catch (Exception e) {log.error(e);}
    }
    
    /**
     * Sets up the connection to the MGD Database.
     * @throws SQLException
     */
    
    private void getMGDConnection() throws SQLException {
        conMGD = DriverManager.getConnection(mgdJDBCUrl, user, password);
    }
    
    /**
     * Clean up the connections to the database, if they have been initialized.
     * @throws SQLException
     */
    
    public void cleanup() throws SQLException {
        if (conMGD != null) {
            conMGD.close();
        }
        if (conSNP != null) {
            conSNP.close();
        }
    }
    
    /**
     * Execute a query against MGD, setting up the connection if needed.
     * @param query
     * @return
     */
    
    public ResultSet executeMGD (String query) {
        
        ResultSet set;
        
        try {
            if (conMGD == null) {
                getMGDConnection();
            }
            
            java.sql.Statement stmt = conMGD.createStatement();
            start = new Date();
            set = stmt.executeQuery(query);
            end = new Date();
            return set;
        } catch (Exception e) {
            log.error(e);
            System.exit(1);
            return null;
        }
    }
    
    /**
     * Get a connection to the SNP database.
     * @throws SQLException
     */
    
    private void getSNPConnection() throws SQLException {
        conSNP = DriverManager.getConnection(snpJDBCUrl, user, password);
    }
    
    /**
     * Perform a query against the SNP Database, initializing a connection
     * if need be.
     * @param query
     * @return
     */
    
    public ResultSet executeSNP (String query) {
        
        ResultSet set;
        
        try {
            if (conSNP == null) {
                getSNPConnection();
            }
            
            java.sql.Statement stmt = conSNP.createStatement();
            start = new Date();
            set = stmt.executeQuery(query);
            end = new Date();
            return set;
        } catch (Exception e) {
            log.error(e);
            System.exit(1);
            return null;
        }
    }
    
    /**
     * Return the timing of the last query.
     * @return
     */
    
    public long getTiming() {
        return end.getTime() - start.getTime();
    }
    
}

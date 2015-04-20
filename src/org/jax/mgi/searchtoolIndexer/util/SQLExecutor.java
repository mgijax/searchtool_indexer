package org.jax.mgi.searchtoolIndexer.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The SQLExecutor class knows how to connect to, and submit queries against
 * the MGD database.
 *
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * @does Executes SQL Queries against MGD
 *
 */

public class SQLExecutor {

	protected Connection conMGD = null;
	private String user;
	private String password;
	private String mgdJDBCUrl;

	private Date start;
	private Date end;

	// now pulled from configuration, rather than hard-coding Sybase
	protected String DB_DRIVER = null;

	protected Logger log = Logger.getLogger(this.getClass().getName());

	/**
	 * The default constructor sets up all the configuration variables from
	 * IndexCfg.
	 *
	 * @param config
	 */

	public SQLExecutor (IndexCfg config) {
		try {
			DB_DRIVER = config.get("DB_DRIVER");
			Class.forName(DB_DRIVER);
			user = config.get("MGI_PUBLICUSER");
			password = config.get("MGI_PUBLICPASSWORD");
			mgdJDBCUrl = config.get("MGD_JDBC_URL");
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
	}

	/**
	 * Execute a query against MGD, setting up the connection if needed.
	 * @param query
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

	public long getTiming() {
		return end.getTime() - start.getTime();
	}

}

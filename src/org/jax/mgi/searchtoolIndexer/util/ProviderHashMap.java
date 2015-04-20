package org.jax.mgi.searchtoolIndexer.util;

import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * This class pulls together the mapping of logical -> actual db's from the
 * database, upon request.
 * 
 * @author mhall
 * 
 * @has An IndexCfg object, which is used 
 * 
 * @does A singleton, that upon instantiation will populate itself with the
 * display text for the various logical providers, with special handling for
 * certain providers.
 * 
 */

public class ProviderHashMap {

    public HashMap<String, String> providerHash = 
        new HashMap<String, String>();

    // Gather all the logical db keys from the database.
    
    private String PROVIDER_SQL = "select _LogicalDB_key, name"
            + " from ACC_LogicalDB";

    // Gather up the exceptions to the logical db rules.  This is is for
    // the compound providers.  (logical databases with >1 actual database)
    
    private String EXCEPTION_PROVIDER_SQL =
	"select distinct _LogicalDB_key, name "
	+ "from ACC_ActualDB a1 "
	+ "where active = 1 "
	+ "  and exists (select 1 from ACC_ActualDB a2 "
	+ "    where a1._LogicalDB_key = a2._LogicalDB_key "
	+ "    and a2.active = 1 "
	+ "    and a1._ActualDB_key != a2._ActualDB_key) "
	+ "order by _LogicalDB_key, name desc";

    private static Logger log = Logger.getLogger(ProviderHashMap.class
            .getName());
    
    // Sybase JDBC driver used to be hard-coded.  We now pull this from 
    // configuration instead.
    protected String DB_DRIVER = null;
    
    protected SQLExecutor executor; 

    /**
     * Constructor, this calls the super class constructor, and then invokes
     * the init method which populates this object.
     * 
     * @param config
     */

    public ProviderHashMap(IndexCfg config) {
        try {
	    DB_DRIVER = config.get("DB_DRIVER");
            executor = new SQLExecutor(config);
        } catch (Exception e) {
            log.error(e);
        }
        init();
    }
    
    /**
     * Populate this object with the providers.
     */

    private void init() {

        // This is the standard case, each provider directly correlates 
        // to a logical db key.

        ResultSet provider_set = executor.executeMGD(PROVIDER_SQL);
        try {
            provider_set.next();

            while (!provider_set.isAfterLast()) {
                providerHash.put(provider_set.getString("_LogicalDB_key"),
                        provider_set.getString("name"));
                provider_set.next();
            }
        } catch (Exception e) {
            log.error(e);
        }

        // Here are the exceptions, these are compound providers, 
        // Since this is a map, we will simply overwrite the values from the
        // original sql.

        ResultSet exception_provider_set = executor.executeMGD(EXCEPTION_PROVIDER_SQL);
        try {
            exception_provider_set.next();

            int current = -1;
            String value = "";

            // Iterate through the logical db dataset.  Setting the display
            // names.
            
            while (!exception_provider_set.isAfterLast()) {
                if (current != (int) exception_provider_set.getInt(1)) {
                    if (current != -1) {
                        providerHash
                                .put(new Integer(current).toString(), value);
                        value = "";
                    }
                }
                if (value.equals("")) {
                    value = exception_provider_set.getString("name");
                    current = exception_provider_set.getInt("_LogicalDB_key");
                } else {
                    value += ", " + exception_provider_set.getString("name");
                }
                exception_provider_set.next();
            }
        } catch (Exception e) {
            log.error(e);
        }

        // Override the "MGI" Type locally to be blank, so we don't have to do
        // this all throughout the display layer.

        providerHash.put("1", "");

    }

    /**
     * Get the logical db display name.
     * @param logicalDB
     */

    public String get(String logicalDB) {
        return providerHash.get(logicalDB);
    }

    /**
     * Set the logical db display name.
     * @param key
     * @param logicalDB
     */

    public void put(String key, String logicalDB) {
        providerHash.put(key, logicalDB);
    }

    /**
     * This is the test harness for this class, assuming you are familiar with
     * this data set, you can run it to verify that the class is functioning
     * properly.
     * 
     * @param args
     */

    public static void main(String[] args) {
        IndexCfg config;

        log.info("Stub for the ProviderHashSet TestHarness");

        try {
            config = new IndexCfg();
            ProviderHashMap phmg = new ProviderHashMap(config);

            log.info("DB Key 9: " + phmg.get("9"));
            log.info("DB Key 2: " + phmg.get("2"));
            log.info("DB Key 3: " + phmg.get("3"));
            log.info("DB Key 13: " + phmg.get("13"));
            log.info("DB Key 70: " + phmg.get("70"));
            log.info("DB Key 1: " + phmg.get("1"));
            log.info("DB Key 16: " + phmg.get("16"));
            log.info("DB Key 41: " + phmg.get("41"));

        } catch (Exception e) {
            log.error(e);
        }
    }

}

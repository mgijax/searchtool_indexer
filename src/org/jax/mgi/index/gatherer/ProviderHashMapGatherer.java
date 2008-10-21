package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.shr.config.Configuration;

/**
 * This class pulls together the mapping of logical -> actual db's from the
 * database, upon request.
 * 
 * @author mhall
 * 
 * @has An internal HashMap which stores the provider relationships Two SQL
 * Statements that gather the information needed to populate this object.
 * 
 * @does A singleton, than upon instantiation will populate itself with the
 * display text for the various logical providers, with special handling for
 * certian providers.
 * 
 */

public class ProviderHashMapGatherer extends AbstractGatherer {

    public HashMap<String, String> providerHash           = new HashMap<String, String>();

    private String                 PROVIDER_SQL           = "select _LogicalDB_key, name"
                                                                  + " from ACC_LogicalDB";

    private String                 EXCEPTION_PROVIDER_SQL = "select distinct _LogicalDB_key, name"
                                                                  + " from ACC_ActualDB"
                                                                  + " where active = 1 "
                                                                  + " group by _LogicalDB_key"
                                                                  + " having count(*) > 1"
                                                                  + " order by _LogicalDB_key, name desc";

    private static Logger log = Logger.getLogger(ProviderHashMapGatherer.class.getName());
    
    /**
     * Constructor, this calls the superclasses constructor, and then invokes the init method
     * which populates this object.
     * 
     * @param config
     */
    
    public ProviderHashMapGatherer(Configuration config) {
        super(config);
        init();
    }

    /**
     * Populate this object with the providers.
     */
    
    private void init() {

        // This is the standard case, each provider directly coorelates to a logical db key.
        
        ResultSet provider_set = execute(PROVIDER_SQL);
        try {
            provider_set.next();

            while (!provider_set.isAfterLast()) {
                providerHash.put(provider_set.getString("_LogicalDB_key"), provider_set
                        .getString("name"));
                provider_set.next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Here are the exceptions, these are compound providers, which are
        // really annoying.
        // Since this is a map, we will simply overwrite the values from the
        // original sql.

        ResultSet exception_provider_set = execute(EXCEPTION_PROVIDER_SQL);
        try {
            exception_provider_set.next();

            int current = -1;
            String value = "";

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
            e.printStackTrace();
        }

        // Override the "MGI" Type locally to be blank, so we don't have to do
        // this all throughout the display layer.

        providerHash.put("1", "");

    }

    // Get a logical DB display name
    
    public String get(String logicalDB) {
        return providerHash.get(logicalDB);
    }

    // Add in a new logical db display name (or overwrite)
    
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
        Configuration config;

        log.info("Stub for the ProviderHashSet TestHarness");

        try {
            config = Configuration.load("Configuration", false);
            ProviderHashMapGatherer phmg = new ProviderHashMapGatherer(config);

            log.info("DB Key 9: " + phmg.get("9"));
            log.info("DB Key 2: " + phmg.get("2"));
            log.info("DB Key 3: " + phmg.get("3"));
            log.info("DB Key 13: " + phmg.get("13"));
            log.info("DB Key 70: " + phmg.get("70"));
            log.info("DB Key 1: " + phmg.get("1"));
            log.info("DB Key 16: " + phmg.get("16"));
            log.info("DB Key 41: " + phmg.get("41"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

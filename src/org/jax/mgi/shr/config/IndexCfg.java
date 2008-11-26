package org.jax.mgi.shr.config;

/**
 * This object extends the back end configuration object, constructing a 
 * configuration object for our code.
 * 
 * @author mhall
 * @has Nothing
 * @does Extends the back end Configurator method, and pulls in the 
 * configuration items from the environment.
 */

public class IndexCfg extends Configurator {
    
    public IndexCfg() throws ConfigException {}
    
    public String get(String item) throws ConfigException{
        return getConfigString(item);
    }

}

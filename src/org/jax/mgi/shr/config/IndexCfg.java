package org.jax.mgi.shr.config;

/**
 * This object extends the backend configuration object, constructing a 
 * configuration object for our code.
 * 
 * @author mhall
 *
 */

public class IndexCfg extends Configurator {
    
    public IndexCfg() throws ConfigException {}
    
    public String get(String item) throws ConfigException{
        return getConfigString(item);
    }

}

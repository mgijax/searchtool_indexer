package org.jax.mgi.shr.config;

public class IndexCfg extends Configurator {
    
    public IndexCfg() throws ConfigException {}
    
    public String get(String item) throws ConfigException{
        return getConfigString(item);
    }

}

package org.jax.mgi.searchtoolIndexer.gatherer;

import org.jax.mgi.searchtoolIndexer.util.SQLExecutor;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The DatabaseGatherer acts as a parent class for all of the currently
 * implemented gatherers in the searchtool.
 * 
 * @author mhall
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * @does Provides common services and a consistent API for all child
 *      gatherers to implement.
 *      This includes:
 *      An SQLExecutor, used to run queries against MGD and SNP
 *      A cleanup method that closes out the excecutor gracefully upon
 *      thread death.
 */

public abstract class DatabaseGatherer extends AbstractGatherer {

    protected SQLExecutor executor;
    
    public DatabaseGatherer(IndexCfg config) {
        super(config);
        executor = new SQLExecutor(config);
    }

    /**
     * Gracefully close the connections after they are finished 
     * being used by the gatherer.
     */

    protected void cleanup() {
        try {
            executor.cleanup();
        } catch (Exception e) {
            log.error(e);
        }
    }
    
    /**
     * Implementing classes must implement thier own run logic.
     */
    
    protected abstract void runLocal() throws Exception;

}

package org.jax.mgi.searchtoolIndexer.gatherer;

import org.jax.mgi.searchtoolIndexer.util.SQLExecutor;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The DatabaseGatherer - parent class for gatherers in the searchtool
 *
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * @does Provides common services and a consistent API for all child
 *      gatherers to implement.
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

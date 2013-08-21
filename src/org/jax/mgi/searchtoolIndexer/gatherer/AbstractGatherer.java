package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.Connection;

import org.apache.log4j.Logger;
import org.jax.mgi.searchtoolIndexer.util.SharedDocumentStack;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The AbstractGatherer class is the superclass for all the gatherers
 * in the indexing software.
 *
 * It provides a templated run method, and a default constructor.
 * @author mhall
 *
 * @has A single instance of the SharedDocumentStack, which is used to hold the
 *       Lucene documents that implementing objects produce.
 *      A IndexCfg Object, used to configure this object and any of
 *       its children.
 *
 * @does Defines the list of common services provided to any gatherer, as well
 *  as enforcing an api on them.
 *
 */

public abstract class AbstractGatherer implements Runnable {

    protected SharedDocumentStack documentStore;
    protected Connection          con;
    protected Integer             stack_max;
    protected Logger log =
        Logger.getLogger(this.getClass().getName());

    /**
     * Superclass constructor, this pulls in a configuration object
     * for any implementing class to use, as well as setting up the stack
     * max and the shared document stack.
     *
     * @param config
     */

    protected AbstractGatherer(IndexCfg config) {
        try {
            stack_max = new Integer(config.get("STACK_MAX"));
        } catch (Exception e) {
            log.error(e);
        }
        documentStore = SharedDocumentStack.getSharedDocumentStack();
	documentStore.setMaxSize(stack_max.intValue());
    }

    /**
     * This method provides the template for run methods, ensuring that the
     * local run method is called, and that cleanup is called upon thread death.
     */

    public void run() {
        try {
            runLocal();
        } catch (Exception e) {
            log.error("Exception caught in Abstract Gatherer run()");
            log.error(e);
        } finally {
            documentStore.setComplete();
            cleanup();
        }
    }

    /**
     * Implementing classes must implement their own logic when the thread is
     * run.
     * @throws Exception
     */

    protected abstract void runLocal() throws Exception;

    /**
     * Implementing classes must implement their own logic for cleanup.
     */

    protected abstract void cleanup();

}

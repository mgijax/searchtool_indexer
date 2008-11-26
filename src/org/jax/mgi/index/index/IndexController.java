package org.jax.mgi.index.index;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The IndexController is responsible for handling the creation of all of the
 * Indexer threads. After it has created them, it starts them all, and waits
 * until they have completed processing. Once processing is complete, it goes
 * ahead and optimized the index, and then exits.
 * 
 * @author mhall
 * 
 * @has Configurable array of Indexers
 * @does Controls the creation of the indexers, and then optimizes the  
 * resulting Lucene index.
 * 
 */

public class IndexController implements Runnable {

    private static int NUMBER_OF_THREADS = 1;
    IndexWriter        writer            = null;
    
    Logger log = Logger.getLogger(IndexController.class.getName());

    /**
     * Sets up the IndexController, initializing it with a IndexWriter.
     * 
     * @param iw A Lucene IndexWriter
     */

    public IndexController(IndexWriter iw) {
        writer = iw;
        try {
            IndexCfg config = new IndexCfg();
            NUMBER_OF_THREADS = 
                new Integer(config.get("NUMBER_OF_THREADS")).intValue();
        }
        catch (Exception e) {
            log.error(e);
        }

    }

    /**
     * This is what is invoked when this thread is started by the main program.
     * It used the configuration file to figure out how many threads it should
     * be creating, and then monitors them as they work. When its complete, it
     * optimizes the index, and then exits.
     */

    public void run() {
        Thread threads[] = new Thread[NUMBER_OF_THREADS];

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            threads[i] = new Thread(new Indexer(writer));
            threads[i].start();
        }

        // Wait until all the threads have completed their work, after which
        // optimize and close the indexwriter.

        try {

            for (int i = 0; i < NUMBER_OF_THREADS; i++) {
                threads[i].join();
            }

            // Optimize the index.

            writer.optimize();

            // Close the index.

            writer.close();
        } catch (Exception e) {
            log.error(e);
        }
    }

}

package org.jax.mgi.searchtoolIndexer.index;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.jax.mgi.searchtoolIndexer.util.SharedDocumentStack;

/**
 * The Indexer class has the primary responsibilty of taking Lucene documents
 * off of the stack, and giving them to the IndexWriter to be indexed.
 * 
 * @author mhall
 * @has A Reference to the current IndexWriter, which is uses to add documents to 
 * the queue of items to be indexed.
 * @does Depopulates the SharedDocumentStack, and adds them to the index, and
 *       knows to stop processing when the stack is empty, and indexing is
 *       complete.
 * 
 */

public class Indexer implements Runnable {

    IndexWriter writer;
    SharedDocumentStack sis;
    Logger log = Logger.getLogger(this.getClass().getName());

    /**
     * The constructor sets up the internal reference to the IndexWriter, and
     * requests a reference to the SharedDocumentStack
     * 
     * @param w
     */

    public Indexer(IndexWriter w) throws Exception {
        // Make the stack wait times configurable?
        //IndexCfg config = new IndexCfg();
        writer = w;
        sis = SharedDocumentStack.getSharedDocumentStack();
    }

    /**
     * Start to remove documents from the stack.  When the stack returns a null
     * document that means that gathering is complete.  So we can then exit.
     */

    
    public void run() {
        Document doc;
        double count = 0;
        double threshold = 10000;

        try {
            while ((doc = (Document) sis.pop()) != null) {
                writer.addDocument(doc);
                count = count + 1;
                if (count >= threshold) {
                    log.debug("Thread: " + this.hashCode() + " has indexed "
                            + count + " documents!");
                    threshold *= 2;
                }
            }

        } catch (Exception e) {
            log.error(e);
        }

    }
    
}
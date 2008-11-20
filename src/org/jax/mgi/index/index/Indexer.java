package org.jax.mgi.index.index;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.jax.mgi.index.util.SharedDocumentStack;

/**
 * The Indexer class has the primary responsibilty of taking Lucene documents
 * off of the stack, and giving them to the IndexWriter to be indexed.
 * 
 * @author mhall
 * @is A Thread
 * @has A Reference to the current IndexWriter
 * @does Depopulates the SharedDocumentStack, and adds them to the index, and
 *       knows to stop processing when the stack is empty, and indexing is
 *       complete.
 * 
 */

public class Indexer implements Runnable {

    IndexWriter writer;
    SharedDocumentStack sis;
    Logger log = Logger.getLogger(Indexer.class.getName());

    /**
     * The constructor sets up the internal reference to the IndexWriter, and
     * requests a reference to the SharedDocumentStack
     * 
     * @param w
     */

    public Indexer(IndexWriter w) {
        writer = w;
        sis = SharedDocumentStack.getSharedDocumentStack();
    }

    /**
     * This is the method that is invoked when the thread is started. 
     * Basically, we busy wait until there are documents on the stack. 
     * Once we find some on the stack, we take one off, and add it to the 
     * IndexWriter. While we are busy waiting we check to see if the stack 
     * says that all documents have been added to it, and to verify if the 
     * stack is empty. If both conditions are true, we exit.
     * 
     */

    public void run() {
        Document doc;
        double count = 0;
        double threshold = 10000;

        while (!sis.isEmpty() || !sis.isComplete()) {
            try {
                if ((doc = (Document) sis.pop()) != null) {
                    writer.addDocument(doc);
                    count = count + 1;
                    if (count >= threshold) {
                        log.debug("Thread: " + this.hashCode() 
                                + " has indexed " + count + " documents!");
                        threshold *= 2;
                    }
                }
            } catch (Exception e) {
                log.error(e);
            }

        }
    }
}
package org.jax.mgi.searchtoolIndexer.util;

import java.util.Stack;

import org.apache.lucene.document.Document;

/**
 * This stack is the repository for all the Lucene documents that are being
 * created during indexing. This is a stateful object, and has a method that
 * both allows the consumers to interrogate the state, as well as the 
 * gatherers to set the state when they have completed their tasks.
 * 
 * The pop, isEmpty() methods are synchronized, so using this
 * object should be thread safe.
 * 
 * @author mhall
 * @has A stack object, and a Boolean to keep track of state.
 * @does Encapsulates the interaction for a shared stack object, enforcing 
 * synchronization, and keeps track of the state of the processing overall.
 * 
 */

public class SharedDocumentStack {

    private static SharedDocumentStack theInstance = new SharedDocumentStack();
    private static Stack<Document>     stack = new Stack<Document>();
    private Boolean                    gatheringComplete = false;

    // Hidden constructor, access to this object is through the singleton 
    // get method.

    private SharedDocumentStack() {
    };

    /**
     * Singleton access method to gain a reference to the SharedDocumentStack
     * 
     * @return SharedDocumentStack
     */

    public static SharedDocumentStack getSharedDocumentStack() {
        return theInstance;
    }

    /**
     * Add a document onto the stack.
     * 
     * @param doc
     * A lucence document to add.
     */

    public void push(Document doc) {
        stack.push(doc);
    }

    /**
     * Take a document off of the stack, access into this method is
     * synchronized, so its thread safe.
     * 
     * @return A Lucene Document
     */

    public synchronized Document pop() throws InterruptedException {
        
        int waittime = 1;
        
        while (!isComplete() || !isEmpty()) {
            if (!isEmpty()) {
                return stack.pop();
            }
            else {
                Thread.sleep(1000*waittime);
                if (waittime < 16) {
                    waittime *= 2;
                }
            }                
        }
        return null;
    }
    
    /**
     * Is the stack empty?
     * 
     * @return Boolean
     */

    public Boolean isEmpty() {
        return stack.isEmpty();
    }

    /**
     * Set that the gathering process is complete.
     */

    public void setComplete() {
        gatheringComplete = true;
    }

    /**
     * Return the size of the stack.
     * 
     * @return Int value for Size
     */

    public int size() {
        return stack.size();
    }

    /**
     * Is gathering complete? This is only used internally by the stack.
     * 
     * @return Boolean indicating state
     */

    private boolean isComplete() {
        return gatheringComplete;
    }
}
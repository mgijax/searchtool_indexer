package org.jax.mgi.index.luceneDocBuilder;

import org.apache.lucene.document.Document;

/**
 * This interface defines the api that all LuceneDocBuilders share.
 * 
 * @author mhall
 *
 * @has nothing
 * @does defines a template for all LuceneDocBuilders to implement.
 */

public interface LuceneDocBuilder {

    /**
     * Implementing objects must know how to return themselves to their 
     * default state.
     */

    public void clear();

    /**
     * Implementing objects must know how to return a Lucene Document from 
     * their contents.
     * 
     * @return A Lucene document representing the contents of the 
     * implementing object.
     */

    public Document getDocument();

    /**
     * Implementing objects must know how to return a String representation 
     * of their contents.
     * 
     * @return A String representing the contents of the implementing object.
     */

    public String toString();

}

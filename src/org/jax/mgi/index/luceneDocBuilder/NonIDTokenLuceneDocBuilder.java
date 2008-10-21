package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import QS_Commons.IndexConstants;

/**
 * Container Class that encapsulates the data needed to create the
 * MarkerAndVocabByField Index
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into
 *  a Lucene document.
 * 
 */

public class NonIDTokenLuceneDocBuilder implements LuceneDocBuilder {

    private String  data        = "";
    private Boolean hasError    = false;
    
    private Logger log = 
        Logger.getLogger(MarkerExactLuceneDocBuilder.class.getName());

    /**
     * Resets the object back to its initial state. This allows us to reuse the
     * same object for our entire indexing process.
     */
    public void clear() {
        this.data       = "";
        this.hasError   = false;
    }

    /**
     * Returns a Lucene document.
     * 
     * @return A Lucene Document representing a MarkerAndVocabByField record.
     */

    public Document getDocument() {

        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();

        doc.add(new Field(IndexConstants.COL_DATA, this.getData(), 
                Field.Store.YES, Field.Index.TOKENIZED));
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "Data: " + this.getData();
    }

    /**
     * Serves as a spot to place a test harness in this object.
     * 
     * @param args
     */

    public static void main(String[] args) {
       // Set up the logger.
        
        Logger log = 
            Logger.getLogger(NonIDTokenLuceneDocBuilder.class.getName());
        
        log.info("NonIDTokenLuceneDocBuilder Test Harness");

        NonIDTokenLuceneDocBuilder ldb = new NonIDTokenLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        ldb.setData(null);
        Document doc = ldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        ldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        ldb.setData("test");
        
        doc = ldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(ldb);
        
        log.info("Lucene document: " + doc);
    }

    /**
     * Appends to the data field. Some of our fields are split up in the
     * database, so they have to be put back together manually at indexing time.
     * 
     * @param data
     */

    public void appendData(String data) {
        if (this.data.equals("")) {
            this.data = data;
        } else {
            this.data = this.data + data;
        }
    }

    /**
     * Returns the data field to be indexed.
     * 
     * @return The string representation of the data to be indexed.
     */

    public String getData() {
        return data;
    }

    /**
     * Sets the data field to be indexed. Defaults to an empty string.
     * 
     * @param data
     */

    public void setData(String data) {
        if (data != null) {
            this.data = data;
        }
        else {
            hasError = true;
        }
            
            
    }

}

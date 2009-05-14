package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Container Class that encapsulates the data needed to create the
 * nonIDToken Index
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into
 *  a Lucene document.
 * 
 */

public class NonIDTokenLuceneDocBuilder extends AbstractLuceneDocBuilder {

    /**
     * This method is required by the LuceneDocBuilder class.
     */
    
    protected void clearLocal() {}

    /**
     * Returns a Lucene document.
     * 
     * @return A Lucene Document representing a MarkerAndVocabByField record.
     */

    protected Document prepareDocument() {

        doc.add(new Field(IndexConstants.COL_DATA, this.data, 
                Field.Store.YES, Field.Index.TOKENIZED));
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "Data: " + this.data;
    }

    /**
     * Appends to the data field. Some of our fields are split up in the
     * database, so they have to be put back together manually at indexing 
     * time.
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
     * Test harness for this object.
     * 
     * @param args
     */
    
    public static void main(String[] args) {
       // Set up the logger.

        NonIDTokenLuceneDocBuilder builder =
            new NonIDTokenLuceneDocBuilder();
        
        Logger log = 
            Logger.getLogger(builder.getClass().getName());
        
        log.info(builder.getClass().getName() + " Test Harness");

        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        builder.setData(null);
        Document doc = builder.getDocument();
        
        // Reset the doc builder for the next object.
        
        builder.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.
    
        builder.setData("test");
        
        doc = builder.getDocument();
    
        // Should print out the toString() version of the doc builder.
        
        log.info(builder);
        
        log.info("Lucene document: " + doc);
    }

}

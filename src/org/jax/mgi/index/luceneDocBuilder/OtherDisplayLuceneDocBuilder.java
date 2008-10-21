package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import QS_Commons.IndexConstants;

/**
 * Object that encapsulates the information needed to create a single document
 * in the VocabExact index.
 * 
 * @author mhall
 *  
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a Lucene document.
 * 
 */
public class OtherDisplayLuceneDocBuilder implements LuceneDocBuilder {

    private String  db_key     = "";
    private String  data_type  = "";
    private String  qualifier  = "";
    private String  name       = "";

    private Logger log         = 
        Logger.getLogger(OtherDisplayLuceneDocBuilder.class.getName());
    
    private Boolean hasError   = false;

    /**
     * Clears the object into its default state. This allows the object to be
     * reused in the indexing process.
     */
    public void clear() {
        this.db_key     = "";
        this.data_type  = "";
        this.qualifier  = "";
        this.name       = "";
        this.hasError   = false;
    }

    /**
     * Returns a Lucene document This object also performs so transformation on
     * the data field, stripping out all unneeded whitespace before sending it
     * off to be indexed.
     * 
     * @return Lucene document constructed with the information that this object
     *         encapsulates.
     */

    public Document getDocument() {

        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getDataType(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_MARKER_NAME, this.getName(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_QUALIFIER1, this.getQualifier(), Field.Store.YES, Field.Index.NO));
        return doc;
    }

    /**
     * Returns a string representation of this objects data.
     */

    public String toString() {
        return "Id: " + this.getDb_key() + " Raw Data Type: " 
            + this.getDataType() + " Name: " + this.getName()
            + " Qualifier: " + this.getQualifier();

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
       // Set up the logger.
        
        Logger log = Logger.getLogger(OtherDisplayLuceneDocBuilder.class.getName());
        
        log.info("OtherDisplayLuceneDocBuilder Test Harness");

        OtherDisplayLuceneDocBuilder meldb = new OtherDisplayLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        meldb.setName(null);
        Document doc = meldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        meldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        meldb.setName("test");
        meldb.setDb_key("123");        
        meldb.setDataType("123test_type");
        meldb.setQualifier("MARKER");
        
        doc = meldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(meldb);
        
        log.info("Lucene document: " + doc);

    }

    /**
     * Returns the database key
     * 
     * @return String representation of the db_key field.
     */

    public String getDb_key() {
        return db_key;
    }

    /**
     * Sets the database key.
     * 
     * @param db_key
     */

    public void setDb_key(String db_key) {
        if (db_key != null) {
            this.db_key = db_key;
        } else {
            this.hasError = true;
        }
    }

    /**
     * Returns the data type field.
     * 
     * @return String
     */

    public String getDataType() {
        return data_type;
    }

    /**
     * Sets the data type
     * @param data String
     */

    public void setDataType(String data) {
        if (data != null) {
            this.data_type = data;
        }
        else {
            this.hasError = true;
        }
    }

    /** 
     * Gets the first qualifier
     * This is qualifier to set the sub type of an object.
     * 
     * @return String
     */

    public String getQualifier() {
        return qualifier;
    }

    /**
     * Sets the first qualifier string.
     * This is the qualifier that shows the sub type of the object.
     * @param q
     */

    public void setQualifier(String q) {

        if (q != null) {
            this.qualifier = q;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the name of the object
     * 
     * @return String
     */

    public String getName() {
        return name;
    }

    /**
     * Sets the name of the object.
     * 
     * @param name
     */

    public void setName(String name) {
        if (name != null) {
            this.name = name;
        } else {
            this.hasError = true;
        }
    }

}
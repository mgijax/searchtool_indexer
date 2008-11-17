package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data needed to create the MarkerExact index.
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a lucene document.
 * 
 * Note: this document outputs a field raw_data, that simply doesn't lowercase the
 * text from the data field.
 * 
 */
public class MarkerExactLuceneDocBuilder implements LuceneDocBuilder {

    // Internal private variables, which contain the data during runtime usage.

    private String  db_key       = "";
    private String  data         = "";
    private String  data_type    = "";
    private String  display_type = "";
    private String  unique_key   = "";
    
    private Logger  log          =
        Logger.getLogger(MarkerExactLuceneDocBuilder.class.getName());
    
    private Boolean hasError     = false;

    /**
     * Resets this object back to its default state. This allows the object to
     * be reused during the indexing process.
     */

    public void clear() {
        this.db_key       = "";
        this.data         = "";
        this.data_type    = "";
        this.display_type = "";
        this.unique_key   = "";
        this.hasError     = false;
    }

    /**
     * Returns a lucene document.
     * 
     * @return A lucene document representing the data contained in this object.
     */

    public Document getDocument() {

        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();
        
        doc.add(new Field(IndexConstants.COL_DATA,
                this.getData().replaceAll("\\s+", " ").replaceAll("^\\s", "").replaceAll("\\s$", "").toLowerCase(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getData(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getDataType(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY, this.getDisplay_type(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.getUnique_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "Id: " + this.getDb_key() + " Data: " 
        + this.getData() + " Data Type: " + this.getDataType()
        + " Unique Key: " + this.getUnique_key()
        + " Display Type: " + this.getDisplay_type();
    }

    /**
     * This main program is a stub for a test harness that can be built to specifically test this object.
     * 
     * @param args Standard argument.
     */
    public static void main(String[] args) {
        
        // Set up the logger.
        
        Logger log = Logger.getLogger(MarkerExactLuceneDocBuilder.class.getName());
        
        log.info("MarkerExactLuceneDocBuilder Test Harness");

        MarkerExactLuceneDocBuilder meldb = new MarkerExactLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        meldb.setData(null);
        Document doc = meldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        meldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        meldb.setData("test");
        meldb.setDb_key("123");
        meldb.setDataType("test type");
        meldb.setDisplay_type("Test: test");
        meldb.setUnique_key("123test_type");
        
        doc = meldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(meldb);
        
        log.info("Lucene document: " + doc);
        
    }

    /**
     * Returns the database key.
     * 
     * @return String representation of the database key.
     */

    public String getDb_key() {
        return db_key;
    }

    /**
     * Sets the database key. Defaults to an empty string.
     * 
     * @param db_key
     */

    public void setDb_key(String db_key) {
        if (db_key != null) {
            this.db_key = db_key;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the data field.
     * 
     * @return String representation of the data to be indexed.
     */

    public String getData() {
        return data;
    }

    /**
     * Sets the data field.
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

    /**
     * Returns the data type.
     * 
     * @return String representation of the data_type field.
     */

    public String getDataType() {
        return data_type;
    }

    /**
     * Sets the data_type field.
     * 
     * @param type
     */

    public void setDataType(String type) {
        if (type != null) {
            this.data_type = type;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the Display Type field.  Valid display types are maintained in the IndexConstants class
     * in QuickSearchCommons
     * @return String representation of the Display Type
     */

    public String getDisplay_type() {
        return display_type;
    }

    /**
     * Sets the display type.
     * @param display_type String that the display type is to be set to.
     */

    public void setDisplay_type(String display_type) {
        if (display_type != null) {
            this.display_type = display_type;
        }
        else {
            this.hasError = true;
        }
    }

    public String getUnique_key() {
        return unique_key;
    }

    public void setUnique_key(String unique_key) {
        if (unique_key != null) {
            this.unique_key = unique_key;
        }
        else {
            this.hasError = true;
        }            
    }
}

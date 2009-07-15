package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data needed to create the markerSymbol index.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *  lucene document.
 * 
 * Note: this document outputs a field raw_data, that simply doesn't lower 
 * case the text from the data field.
 * 
 */
public class MarkerSymbolLuceneDocBuilder extends AbstractLuceneDocBuilder {

    // Internal private variables, which contain the data during runtime usage.

    private String  data_type    = "";
    private String  display_type = "";
    private String  unique_key   = "";

    /**
     * Resets this object back to its default state. This allows the object to
     * be reused during the indexing process.
     */

    protected void clearLocal() {
        this.data_type    = "";
        this.display_type = "";
        this.unique_key   = "";
    }

    /**
     * Returns a lucene document.
     * 
     * This object also performs a transformation on the data before the
     * document is created.  We remove any extra whitespace, and then 
     * lowercase the data.
     * 
     * @return A lucene document representing the data contained in this 
     * object.
     */

    protected Document prepareDocument() {
        
        doc.add(new Field(IndexConstants.COL_DATA, 
                this.data.replaceAll("\\s+", " ").replaceAll("^\\s", "")
                .replaceAll("\\s$", "").toLowerCase(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        // This index uses a raw_data field, but we don't actually transform
        // the normal data field anymore.  (Since this was split out)
        // As such it remains in for now, but perhaps could be redesigned
        // at some later point.
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.data,
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY, this.display_type,
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.unique_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "Id: " + this.db_key + " Data: " 
        + this.data + " Data Type: " + this.data_type
        + " Unique Key: " + this.unique_key
        + " Display Type: " + this.display_type;
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
     * Returns the Display Type field.  Valid display types are maintained 
     * in the IndexConstants class in QuickSearchCommons.
     * 
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

    /**
     * Return the calculated unique key for this object.  This is in place to 
     * allow a join between indexes at display time.
     * 
     * @return String with this objects unique key.
     */
    
    public String getUnique_key() {
        return unique_key;
    }

    /**
     * Set the unique key for this object.  This is used to create a join point
     * across indexes at display time.
     * 
     * @param unique_key
     */
    
    public void setUnique_key(String unique_key) {
        if (unique_key != null) {
            this.unique_key = unique_key;
        }
        else {
            this.hasError = true;
        }            
    }

    /**
     * This main program is a test harness for the MarkerSymbolLuceneDocBuilder
     * object.
     * 
     * @param args Standard argument.
     */
    public static void main(String[] args) {
        
        // Set up the logger.
        
        MarkerSymbolLuceneDocBuilder builder =
            new MarkerSymbolLuceneDocBuilder();
        
        Logger log = 
            Logger.getLogger(builder.getClass().getName());
        
        log.info(builder.getClass().getName()+ " Test Harness");
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        builder.setData(null);
        Document doc = builder.getDocument();
        
        // Reset the doc builder for the next object.
        
        builder.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.
    
        builder.setData("test");
        builder.setDb_key("123");
        builder.setDataType("test type");
        builder.setDisplay_type("Test: test");
        builder.setUnique_key("123test_type");
        
        doc = builder.getDocument();
    
        // Should print out the toString() version of the doc builder.
        
        log.info(builder);
        
        log.info("Lucene document: " + doc);
        
    }
}

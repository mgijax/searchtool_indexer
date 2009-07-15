package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data needed to create the markerAccID index 
 * documents.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *  lucene document.
 * 
 * Note: this document outputs a field raw_data, that simply doesn't lower case
 * the text from the data field.
 * 
 */
public class MarkerAccIDLuceneDocBuilder extends AbstractLuceneDocBuilder {

    // Internal private variables, which contain the data during runtime usage.

    private String data_type    = "";
    private String provider     = "";
    private String display_type = "";
    
    /**
     * Resets this object back to its default state. This allows the object to
     * be reused during the indexing process.
     */

    protected void clearLocal() {
        this.data_type      = "";
        this.provider       = "";
        this.display_type   = "";
    }

    /**
     * Returns a Lucene document.
     * 
     * This object also proforms some transformation on the data itself,
     * removing all extra whitespace from the data, and lowercasing it.
     * 
     * @return A Lucene document representing the data contained in this 
     * object.
     */

    protected Document prepareDocument() {
        
        doc.add(new Field(IndexConstants.COL_DATA, 
                this.data.replaceAll("\\s+", " ").replaceAll("^\\s", "")
                .replaceAll("\\s$", "").toLowerCase(), 
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.data,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_PROVIDER, this.provider,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY, this.display_type,
                Field.Store.YES, Field.Index.NO));
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "Id: " + this.db_key + " Data: " + this.data
                + " Data Type: " + this.data_type + " Provider: "
                + this.provider + " Display Type: "
                + this.display_type;
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
     * Gets the logical db field.
     * 
     * @return A string containing the logical db information.
     */

    public String getProvider() {
        return provider;
    }

    /**
     * Set the logical db field.
     * @param provider
     */

    public void setProvider(String provider) {
        if (provider != null) {
            this.provider = provider;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the Display Type field.  Valid display types are maintained in 
     * the IndexConstants class in QuickSearchCommons.
     * 
     * @return String representing the DisplayType
     */

    public String getDisplay_type() {
        return display_type;
    }

    /**
     * Sets the display type.
     * @param display_value String that the display type is to be set to.
     */

    public void setDisplay_type(String display_value) {
        if (display_value != null) {
            this.display_type = display_value;
        }
        else {
            this.hasError = true;
        }
    }
    
    /**
     * This main program is a stub for a test harness that can be built to 
     * specifically test this object.
     * 
     * @param args Standard argument.
     */
    public static void main(String[] args) {
        // Set up the logger.

        MarkerAccIDLuceneDocBuilder builder = new MarkerAccIDLuceneDocBuilder();
        
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
        builder.setDb_key("123");
        builder.setDataType("test type");
        builder.setDisplay_type("Test: test");
        builder.setProvider("test provider");
        
        doc = builder.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(builder);
        
        log.info("Lucene document: " + doc);

    }
}

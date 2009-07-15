 package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object encapsulating the data required to populate the vocabInexact Index.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into 
 * a lucene document.
 * 
 */

public class VocabInexactLuceneDocBuilder extends AbstractLuceneDocBuilder {

    // Private variables
    
    private String  vocabulary   = "";
    private String  data_type    = "";
    private String  raw_data     = "";
    private String  display_type = "";
    private String  unique_key   = "";

    /**
     * Returns the object to its default state. This allows reuse of this 
     * object during the indexing process.
     */
    
    protected void clearLocal() {
        this.vocabulary     = "";
        this.data_type      = "";
        this.raw_data       = "";
        this.display_type   = "";
        this.unique_key     = "";
    }

    /**
     * Returns a Lucene document
     * 
     * This index has special handling where it transforms all non standard 
     * white space into a single space.
     * 
     * @return Lucene document representing the data contained in this object.
     */
    
    protected Document prepareDocument() {
    
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DATA,
                this.data.replaceAll("[\\W_]", " "),
                Field.Store.YES, Field.Index.TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.raw_data,
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_SDATA,
                this.data.replaceAll("[\\W_]", " "),
                Field.Store.YES, Field.Index.TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_VOCABULARY, this.vocabulary,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.display_type, Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.unique_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * String representation of the data contained in this object
     */
    
    public String toString() {
        return "Id: " + this.db_key + "  Data: " + this.data 
            + "  Type: " + this.vocabulary + "  Data Type: " 
            + this.data_type + "  Raw Data: " + this.raw_data
            + "  Unique Key: " + this.unique_key;
    }

    /**
     * Appends data to the field to be searched. Since the data in the database
     * could be compound in nature, you have to have the ability to put this
     * field together.
     * 
     * @param data
     */
    
    public void appendData(String data) {
        if (data != null) {
            if (this.data.equals("")) {
                this.data = data;
            } else {
                this.data = this.data + data;
            }
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the vocabulary.
     * 
     * @return String representation
     */
    
    public String getVocabulary() {
        return vocabulary;
    }

    /**
     * Set the vocabulary This function will check for the irregularly long
     * vocabulary names, and convert them to a shortened format for the 
     * indexes.
     * 
     * @param type
     */
    
    public void setVocabulary(String type) {
        if (type != null) {
            if (type.equals("Mammalian Phenotype")) {
                type = "MP";
            }
            if (type.equals("InterPro Domains")) {
                type = "IP";
            }
            if (type.equals("PIR Superfamily")) {
                type = "PS";
            }
            this.vocabulary = type;
        }
        else {
            this.hasError = true;
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
     * Sets the data_type variable. This is the internal representation of the
     * vocabulary type.
     * 
     * @param vocType
     */
    
    public void setDataType(String vocType) {
        if (data_type != null) {
            this.data_type = vocType;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Return the raw representation of the data.
     * 
     * @return String
     */
    
    public String getRaw_data() {
        return raw_data;
    }

    /**
     * Sets the raw data, this data will not be put through analysis at 
     * indexing time.
     * 
     * @param raw_data
     */
    
    public void setRaw_data(String raw_data) {
        if (raw_data != null) {
            this.raw_data = raw_data;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the display type
     * @return String 
     */
    
    public String getDisplay_type() {
        return display_type;
    }

    /**
     * Set the display type
     * @param display_type
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
     * Return the unique key for this document.  The key is used to join
     * across indexes.
     * @return String with this objects unique key.
     */
    
    public String getUnique_key() {
        return unique_key;
    }

    
    /**
     * Set the unique key for this document.  The key is used to join across
     * indexes.
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
     * Test harness for this object.
     * 
     * @param args
     */
    public static void main(String[] args) {
        // Set up the logger.
        
        VocabInexactLuceneDocBuilder builder =
            new VocabInexactLuceneDocBuilder();
        
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
        builder.setUnique_key("123test_type");
        builder.setVocabulary("MARKER");
        
        doc = builder.getDocument();
    
        // Should print out the toString() version of the doc builder.
        
        log.info(builder);
        
        log.info("Lucene document: " + doc);
    
    }
}


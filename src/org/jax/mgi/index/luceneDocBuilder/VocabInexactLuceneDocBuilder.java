 package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object encapsulating the data required to populate the VocabByField Index.
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into 
 * a lucene document.
 * 
 */

public class VocabInexactLuceneDocBuilder implements LuceneDocBuilder {

    // Private variables
    
    private String  data         = "";
    private String  db_key       = "";
    private String  vocabulary   = "";
    private String  data_type    = "";
    private String  raw_data     = "";
    private String  display_type = "";
    private String  unique_key   = "";
    
    private Logger  log          = 
        Logger.getLogger(MarkerVocabDagLuceneDocBuilder.class.getName());
    
    private Boolean hasError     = false;

    /**
     * Returns the object to its default state. This allows reuse of this 
     * object during the indexing process.
     */
    
    public void clear() {
        this.data           = "";
        this.db_key         = "";
        this.vocabulary     = "";
        this.data_type      = "";
        this.raw_data       = "";
        this.display_type   = "";
        this.unique_key     = "";
        this.hasError       = false;
    }

    /**
     * Returns a Lucene document
     * 
     * @return Lucene document representing the data contained in this object.
     */
    
    public Document getDocument() {
    
        // Do we have an error? If so dump the contents of this object to the
        // logs.
        
        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();
    
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DATA,
                this.getData().replaceAll("\\W", " "),
                Field.Store.YES, Field.Index.TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getRaw_data(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_SDATA,
                this.getData().replaceAll("\\W", " "),
                Field.Store.YES, Field.Index.TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_VOCABULARY, this.getVocabulary(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getDataType(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.getDisplay_type(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.getUnique_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * String representation of the data contained in this object
     */
    
    public String toString() {
        return "Id: " + this.getDb_key() + "  Data: " + this.getData() 
            + "  Type: " + this.getVocabulary() + "  Data Type: " 
            + this.getDataType() + "  Raw Data: " + this.getRaw_data()
            + "  Unique Key: " + this.getUnique_key();
    }

    /**
     * This is used as a stub for a test harness for this object.
     * 
     * @param args
     */
    public static void main(String[] args) {
        // Set up the logger.
        
        Logger log = 
            Logger.getLogger(VocabInexactLuceneDocBuilder.class.getName());
        
        log.info("VocabInexactLuceneDocBuilder Test Harness");

        VocabInexactLuceneDocBuilder ldb = new VocabInexactLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        ldb.setData(null);
        Document doc = ldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        ldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        ldb.setData("test");
        ldb.setDb_key("123");
        ldb.setDataType("test type");
        ldb.setDisplay_type("Test: test");
        ldb.setUnique_key("123test_type");
        ldb.setVocabulary("MARKER");
        
        doc = ldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(ldb);
        
        log.info("Lucene document: " + doc);
    
    }

    /**
     * Returns the data to be indexed.
     * 
     * @return String representation of the data field.
     */
    
    public String getData() {
        return data;
    }

    /**
     * Sets the data to be indexed.
     * 
     * @param data
     */
    
    public void setData(String data) {
        if (data != null) {
            this.data = data;
        }
        else {
            this.hasError = true;
        }
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
     * Returns the database key.
     * 
     * @return String representaion of the db_key field.
     */
    
    public String getDb_key() {
        return db_key;
    }

    /**
     * Sets the database key This defaults to an empty string.
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
     * @return
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
}


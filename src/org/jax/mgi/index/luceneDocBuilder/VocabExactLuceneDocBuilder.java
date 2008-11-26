package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the information needed to create a single document
 * in the VocabExact index.
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a lucene document.
 * 
 */
public class VocabExactLuceneDocBuilder implements LuceneDocBuilder {

    private String  db_key       = "";
    private String  data         = "";
    private String  data_type    = "";
    private String  vocabulary   = "";
    private String  raw_data     = "";
    private String  display_type = "";
    private String  provider     = "";
    private String  unique_key   = "";
    
    private Logger  log          = 
        Logger.getLogger(MarkerVocabDagLuceneDocBuilder.class.getName());
    
    private Boolean hasError     = false;

    /**
     * Clears the object into its default state. This allows the object to be
     * reused in the indexing process.
     */
    public void clear() {
        this.db_key         = "";
        this.data           = "";
        this.data_type      = "";
        this.vocabulary     = "";
        this.raw_data       = "";
        this.display_type   = "";
        this.provider       = "";
        this.unique_key     = "";
        this.hasError       = false;
    }

    /**
     * Returns a Lucene document This object also performs so transformation on
     * the data field, stripping out all unneeded whitespace before sending it
     * off to be indexed.
     * 
     * @return Lucene document constructed with the information that this 
     * object
     *         encapsulates.
     */

    public Document getDocument() {

        // Do we have an error? If so dump the contents of this object to the
        // logs.
        
        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();
        
        doc.add(new Field(IndexConstants.COL_DATA,
                this.getData().replaceAll("\\s+", " ")
                .replaceAll("^\\s", "").replaceAll("\\s$", "").toLowerCase(),
                     Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getRaw_data(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_VOCABULARY, this.getVocabulary(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getDataType(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.getDisplay_type(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_PROVIDER, this.getProvider(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.getUnique_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * Returns a string representation of this objects data.
     */

    public String toString() {
        return "Id: " + this.getDb_key() + "  Raw Data: " + this.getData() 
        + "  Data Type: " + this.getDataType() + "  Vocabulary: " 
        + this.getVocabulary()+ "  Unique Key: " + this.getUnique_key();
    }

    /**
     * The Main method is used as a test harness for this object.
     * @param args
     */
    public static void main(String[] args) {
        // Set up the logger.
        
        Logger log = 
            Logger.getLogger(VocabExactLuceneDocBuilder.class.getName());
        
        log.info("VocabExactLuceneDocBuilder Test Harness");

        VocabExactLuceneDocBuilder veldb = new VocabExactLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        veldb.setData(null);
        Document doc = veldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        veldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        veldb.setData("test");
        veldb.setDb_key("123");
        veldb.setDataType("test type");
        veldb.setDisplay_type("Test: test");
        veldb.setUnique_key("123test_type");
        veldb.setVocabulary("MARKER");
        
        doc = veldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(veldb);
        
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
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the searchable data field.
     * 
     * @return String representation of the raw data field.
     */

    public String getData() {
        return data;
    }

    /**
     * Sets the searchable data field.
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
     * Appends to the searchable data field. Some of the data that we are
     * pulling from the database can be compound in nature. So we need to be
     * able to construct a single field in lucene from this.
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
     * Returns the data type (Term Type)
     * 
     * @return String representation of the data_type field.
     */

    public String getDataType() {
        return data_type;
    }

    /**
     * Sets the data type (Term Type)
     * 
     * @param type
     */

    public void setDataType(String type) {
        if (data_type != null) {
            this.data_type = type;
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
     * 
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
     * Returns the vocabulary.
     * 
     * @return String representation of the vocabulary field.
     */

    public String getVocabulary() {
        return vocabulary;
    }

    /**
     * Sets the vocabulary field. If the value passed to this function is
     * "Mammalian Phenotype" then it transforms that data into "MP" for naming
     * consistency in the indexes.
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
     * Get the raw data
     * @return String
     */

    public String getRaw_data() {
        return raw_data;
    }

    /**
     * Set the raw data, this field does not get passed through an analyzer.
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

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        if (provider != null) {
            this.provider = provider;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the unique key for this document.  This is used as a join point
     * across indexes at display time.
     * @return
     */
    
    public String getUnique_key() {
        return unique_key;
    }
    
    /**
     * Sets the unique key for this document.  This is used as a join point 
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
}

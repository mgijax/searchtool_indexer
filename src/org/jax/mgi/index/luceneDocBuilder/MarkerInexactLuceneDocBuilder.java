package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Container Class that encapsulates the data needed to create the
 * MarkerAndVocabByField Index
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a lucene document.
 * 
 */

public class MarkerInexactLuceneDocBuilder implements LuceneDocBuilder {

    private String data         = "";
    private String db_key       = "";
    private String data_type    = "";
    private String isCurrent    = "1";
    private String organism     = "1";
    private String vocabulary   = "";
    private String raw_data     = "";
    private String display_type = "";
    private String unique_key   = "";
    
    private Logger log = Logger.getLogger(MarkerExactLuceneDocBuilder.class.getName());
    
    private Boolean hasError     = false;

    /**
     * Resets the object back to its initial state. This allows us to reuse the
     * same object for our entire indexing process.
     */
    public void clear() {
        this.data           = "";
        this.db_key         = "";
        this.data_type      = "";
        this.isCurrent      = "1";
        this.organism       = "1";
        this.vocabulary     = "";
        this.raw_data       = "";
        this.display_type   = "";
        this.unique_key     = "";   
        this.hasError       = false;
        }

    /**
     * Returns a lucene document.
     * 
     * @return A Lucene Document representing a MarkerAndVocabByField record.
     */

    public Document getDocument() {

        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();

        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DATA, this.getData().replaceAll("\\W", " "), Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getRaw_data(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_SDATA, this.getData().replaceAll("\\W", " "), Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getDataType(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_IS_CURRENT, this.getIsCurrent(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_ORGANISM, this.getOrganism(), Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field(IndexConstants.COL_OBJ_TYPE, this.getVocabulary(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY, this.getDisplay_type(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.getUnique_key(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        return doc;
    }

    /**
     * Returns a string representation of the data contained in this object.
     */

    public String toString() {
        return "DB Key: " + this.getDb_key() + " Data: " + this.getData()
             + " Type: " + this.getDataType() + " Current: "
             + this.getIsCurrent() + " Organism Type: " + this.getOrganism()
             + " Object/Vocabulary Type: " + this.getVocabulary() + " Raw Data: "
             + this.getRaw_data() + " Unique Key: " + this.getUnique_key();
    }

    /**
     * Serves as a spot to place a test harness in this object.
     * 
     * @param args
     */

    public static void main(String[] args) {
       // Set up the logger.
        
        Logger log = Logger.getLogger(MarkerInexactLuceneDocBuilder.class.getName());
        
        log.info("MarkerExactLuceneDocBuilder Test Harness");

        MarkerInexactLuceneDocBuilder meldb = new MarkerInexactLuceneDocBuilder();
        
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
        meldb.setOrganism("test organism");
        meldb.setVocabulary("MARKER");
        
        doc = meldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(meldb);
        
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
            this.hasError = true;
        }
    }

    /**
     * Returns the db_key.
     * 
     * @return The database key.
     */

    public String getDb_key() {
        return db_key;
    }

    /**
     * Sets the db_key. Defaults to an empty string.
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
     * Returns the data_type.
     * 
     * @return Returns a string representation of the term type.
     */

    public String getDataType() {
        return data_type;
    }

    /**
     * Sets the datatype. Defaults to an empty string.
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
     * Returns the whether or not this object is current in the database.
     * 
     * @return String representation of the isCurrent field.
     */

    public String getIsCurrent() {
        return isCurrent;
    }

    /**
     * Sets the isCurrent field. This defaults to 1, or true.
     * 
     * @param isCurrent
     */

    public void setIsCurrent(String isCurrent) {
        if (isCurrent != null) {
            this.isCurrent = isCurrent;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Gets the organism field.
     * 
     * @return String representation of the organism field.
     */
    public String getOrganism() {
        return organism;
    }

    /**
     * Sets the organism field Defaults to 1, which represents mouse.
     * 
     * @param organism
     */

    public void setOrganism(String organism) {
        if (organism != null) {
            this.organism = organism;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Gets the vocabulary type.
     * 
     * @return vocabulary type
     */

    public String getVocabulary() {
        return vocabulary;
    }

    /**
     * Sets that vocabulary type. Defaults to an empty string.
     * 
     * @param type
     * Takes the type from the database, and records it into the object. If the
     * type is Mammalian Phenotype it does a conversion into MP
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
     * Get the raw data field, this is the raw value from the database
     * 
     * @return raw data String.
     */

    public String getRaw_data() {
        return raw_data;
    }

    /**
     * Sets the raw data field, this is used at display time to see an unaltered
     * form of the item we are matching on.
     * 
     * @param raw_data
     * String
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
     * Gets the display type
     * 
     * @return Display Type String
     */

    public String getDisplay_type() {
        return display_type;
    }

    /**
     * Sets the display type.
     * 
     * @param display_type
     * String
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

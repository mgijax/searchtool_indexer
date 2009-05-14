package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the information needed to create a single document
 * in the otherExact index.
 * 
 * @author mhall
 *
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into 
 * a lucene document.
 * 
 */
public class OtherExactLuceneDocBuilder extends AbstractLuceneDocBuilder {

    private String accession_key = "";
    private String type          = "";
    private String organism      = "";
    private String preferred     = "";
    private String provider      = "";
    private String display_type  = "ID";

    /**
     * Clears the object into its default state. This allows the object to be
     * reused in the indexing process.
     */

    protected void clearLocal() {
        this.accession_key  = "";
        this.type           = "";
        this.organism       = "";
        this.preferred      = "";
        this.provider       = "";
        this.display_type   = "ID";
    }

    /**
     * Returns a Lucene document This object also performs so transformation on
     * the data field, stripping out all uneeded whitespace, and then 
     * lowercasing the data before sending it
     * off to be indexed.
     * 
     * @return Lucene document constructed with the information that this 
     * object encapsulates.
     */

    protected Document prepareDocument() {

        doc.add(new Field(IndexConstants.COL_DATA, 
                this.data.replaceAll("\\s+", " ").replaceAll("^\\s", "")
                .replaceAll("\\s$", "").toLowerCase(), Field.Store.YES, 
                Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.data,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_ACC_KEY, this.accession_key,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_PREFERRED, this.preferred,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_PROVIDER, this.provider,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.display_type, Field.Store.YES, Field.Index.NO));
        return doc;
    }

    /**
     * Returns a string representation of this objects data.
     */

    public String toString() {
        return "Id: " + this.db_key + " Raw Data: " + this.data 
            + " Type: " + this.type + " Accession Key: " 
            + this.accession_key + " Organism: " + this.organism 
            + " Preffered: " + this.preferred;

    }

    /**
     * Get the Accession Key
     * 
     * @return String
     */

    public String getAccessionKey() {
        return accession_key;
    }

    /**
     * Set the accession key
     * 
     * @param key
     */

    public void setAccessionKey(String key) {
        if (key != null) {
            this.accession_key = key;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Return the type (perhaps this needs to be renamed?)
     * 
     * @return String
     */

    public String getType() {
        return type;
    }

    /**
     * Set the type.
     * 
     * @param type
     */

    public void setType(String type) {
        if (type != null) {
            this.type = type;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the organism of this record.
     * 
     * @return String 
     */

    public String getOrganism() {
        return organism;
    }

    /**
     * Set the organism of this record.
     * 
     * @param org
     */

    public void setOrganism(String org) {
        if (org != null) {
            this.organism = org;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get whether or not this is the preferred nomenclature for this object
     * 
     * @return String
     */

    public String getPreferred() {
        return preferred;
    }

    /**
     * Set whether or not this is the preferred nomenclature for this object.
     * 
     * @param preferred
     * String
     */

    public void setPreferred(String preferred) {
        if (preferred != null) {
            this.preferred = preferred;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the logical db for this item. This is a realized field that's
     * populated during indexing.
     * 
     * @return String
     */

    public String getProvider() {
        return provider;
    }

    /**
     * Set the logical db, this is a realized field populated during indexing.
     * 
     * @param logical_db
     */

    public void setProvider(String logical_db) {
        if (logical_db != null && ! logical_db.equals("")) {
            this.provider = "("+logical_db+")";
        }
        else if (logical_db == null){
            this.hasError = true;
        }
    }

    /**
     * Get the display type
     * 
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
     * Test harness for this object.
     * 
     * @param args
     */
    public static void main(String[] args) {
       // Set up the logger.

        OtherExactLuceneDocBuilder builder = new OtherExactLuceneDocBuilder();
        
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
        builder.setAccessionKey("123123456");
        builder.setType("test");
        builder.setOrganism("testorg");
        builder.setPreferred("yes");
        builder.setProvider("testprovider");
        builder.setDisplay_type("display type test");
        
        doc = builder.getDocument();
    
        // Should print out the toString() version of the doc builder.
        
        log.info(builder);
        
        log.info("Lucene document: " + doc);
    
    }
}


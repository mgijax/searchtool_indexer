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
 * @does Knows how to take the data contained inside of it, and turn it into 
 * a lucene document.
 * 
 */
public class OtherExactLuceneDocBuilder implements LuceneDocBuilder {

    private String db_key        = "";
    private String data          = "";
    private String accession_key = "";
    private String type          = "";
    private String organism      = "";
    private String preferred     = "";
    private String provider      = "";
    private String display_type  = "ID";
    
    private Logger log = 
        Logger.getLogger(OtherExactLuceneDocBuilder.class.getName());
    
    private Boolean hasError   = false;

    /**
     * Clears the object into its default state. This allows the object to be
     * reused in the indexing process.
     */

    public void clear() {
        this.db_key         = "";
        this.data           = "";
        this.accession_key  = "";
        this.type           = "";
        this.organism       = "";
        this.preferred      = "";
        this.provider       = "";
        this.display_type   = "ID";
        this.hasError       = false;
    }

    /**
     * Returns a Lucene document This object also performs so transformation on
     * the data field, stripping out all uneeded whitespace before sending it
     * off to be indexed.
     * 
     * @return Lucene document constructed with the information that this 
     * object
     * encapsulates.
     */

    public Document getDocument() {

        // Do we have an error? If so dump the contents of this object to the
        // logs.
        
        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();
        doc.add(new Field(IndexConstants.COL_DATA, 
                this.getData().replaceAll("\\s+", " ").replaceAll("^\\s", "")
                .replaceAll("\\s$", "").toLowerCase(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getData(),
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_ACC_KEY, this.getAccessionKey(),
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.getType(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(),
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_PREFERRED, this.getPreferred(),
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_PROVIDER, this.getProvider(),
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.getDisplay_type(), Field.Store.YES, Field.Index.NO));
        return doc;
    }

    /**
     * Returns a string representation of this objects data.
     */

    public String toString() {
        return "Id: " + this.getDb_key() + " Raw Data: " + this.getData() 
            + " Type: " + this.getType() + " Accession Key: " 
            + this.getAccessionKey() + " Organism: " + this.getOrganism() 
            + " Preffered: " + this.getPreferred();

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
       // Set up the logger.
        
        Logger log = 
            Logger.getLogger(OtherExactLuceneDocBuilder.class.getName());
        
        log.info("OtherExactLuceneDocBuilder Test Harness");

        OtherExactLuceneDocBuilder oeldb = new OtherExactLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        oeldb.setData(null);
        Document doc = oeldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        oeldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        oeldb.setData("test");
        oeldb.setDb_key("123");        
        oeldb.setAccessionKey("123123456");
        oeldb.setType("test");
        oeldb.setOrganism("testorg");
        oeldb.setPreferred("yes");
        oeldb.setProvider("testprovider");
        oeldb.setDisplay_type("display type test");
        
        doc = oeldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(oeldb);
        
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
}


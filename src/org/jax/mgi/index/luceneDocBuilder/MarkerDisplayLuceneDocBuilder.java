package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data used to create the MarkerDisplay index.
 * 
 * @author mhall
 * @is a LuceneDocBuilder
 * @has Nothing
 * @does Knows how to turn the data values inside of it into a Lucene document.
 */

public class MarkerDisplayLuceneDocBuilder implements LuceneDocBuilder {

    // Internal private variables, which contain the data during runtime usage.

    private String symbol      = "";
    private String chr         = "UNKNOWN";
    private String db_key      = "";
    private String name        = "";
    private String marker_type = "";
    private String acc_id      = "";
    private Boolean hasError   = false;
    
    private Logger log = Logger.getLogger(MarkerDisplayLuceneDocBuilder.class.getName());

    /**
     * Returns the MarkerDisplay object to its default state. This enables the
     * object to be reused during indexing.
     */

    public void clear() {
        this.symbol         = "";
        this.chr            = "UNKNOWN";
        this.db_key         = "";
        this.marker_type    = "";
        this.name           = "";
        this.acc_id         = "";
        this.hasError       = false;
    }

    /**
     * Returns a lucene document.
     * 
     * @return Lucene document representing the data contained in this object.
     */

    public Document getDocument() {

        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();

        doc.add(new Field(IndexConstants.COL_MARKER_SYMBOL, this.getSymbol(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_CHROMOSOME, this.getChr(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_MARKER_NAME, this.getName(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_MARKER_TYPE, this.getMarker_type(), Field.Store.YES,Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_MGI_ID, this.getAcc_id(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        return doc;
    }

    /**
     * Print out a string representation of this object.
     */

    public String toString() {
        return "Symbol: " + this.getSymbol() + "DB Key: " + this.getDb_key()
                + " Name: " + this.getName() + " Chromosome: " + this.getChr()
                + " Marker Type: " + this.getMarker_type() + " Acc ID: "
                + this.getAcc_id();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // Set up the logger.
        
        Logger log = Logger.getLogger(MarkerDisplayLuceneDocBuilder.class.getName());
        
        log.info("MarkerDisplayLuceneDocBuilder Test Harness");

        MarkerDisplayLuceneDocBuilder meldb = new MarkerDisplayLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        meldb.setSymbol(null);
        Document doc = meldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        meldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        meldb.setSymbol("test");
        meldb.setDb_key("123");
        meldb.setName("test type");
        meldb.setChr("3");
        meldb.setMarker_type("Test Type");
        meldb.setAcc_id("testaccid");
        
        doc = meldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(meldb);
        
        log.info("Lucene document: " + doc);    }

    /**
     * Returns the marker symbol.
     * 
     * @return String representation of the marker symbol.
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * Sets the marker symbol.
     * 
     * @param symbol
     */

    public void setSymbol(String symbol) {
        if (symbol != null) {
            this.symbol = symbol;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the chromosome.
     * 
     * @return String representation of the chromosome field.
     */

    public String getChr() {
        return chr;
    }

    /**
     * Sets the chromosome field.
     * 
     * @param chr
     */

    public void setChr(String chr) {
        if (chr != null) {
            this.chr = chr;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the db key.
     * 
     * @return String representation of the db_key field.
     */

    public String getDb_key() {
        return db_key;
    }

    /**
     * Sets the db_key field.
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
     * Returns the name.
     * 
     * @return String representation of the name field.
     */

    public String getName() {
        return name;
    }

    /**
     * Sets the name field.
     * 
     * @param name - String to set the name field to.
     */

    public void setName(String name) {
        if (name != null) {
            this.name = name;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the marker_type.
     * 
     * @return String representation of the marker_type.
     */

    public String getMarker_type() {
        return marker_type;
    }

    /**
     * Sets the marker_type field.
     * 
     * @param marker_type String to set the marker_type to.
     */

    public void setMarker_type(String marker_type) {
        if (marker_type != null) {
            this.marker_type = marker_type;
        }
        else {
            this.hasError = true;
        }
    }

    public String getAcc_id() {
        return acc_id;
    }

    public void setAcc_id(String acc_id) {
        if (this.acc_id != null) {
            this.acc_id = acc_id;
        }
        else {
            this.hasError = true;
        }
    }
}

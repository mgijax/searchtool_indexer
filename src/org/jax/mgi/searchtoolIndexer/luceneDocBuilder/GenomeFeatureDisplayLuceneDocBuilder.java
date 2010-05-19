package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data used to create the markerDisplay index
 * documents.
 *
 * @author mhall
 *
 * @has Nothing
 * @does Knows how to turn the data values inside of it into a Lucene document.
 */

public class GenomeFeatureDisplayLuceneDocBuilder extends AbstractLuceneDocBuilder {

    // Internal private variables, which contain the data during runtime usage.
    private String symbol      = "";
    private String chr         = "UNKNOWN";
    private String name        = "";
    private String marker_type = "";
    private String acc_id      = "";
/*    private String startCoord  = "";
    private String stopCoord   = "";*/
    private String strand      = "";
    private String locDisplay  = "";
    private String objectType  = "";
    private String batchValue  = "";



    /**
     * Returns the MarkerDisplay object to its default state. This enables the
     * object to be reused during indexing.
     */
    protected void clearLocal() {
        this.symbol         = "";
        this.chr            = "UNKNOWN";
        this.marker_type    = "";
        this.name           = "";
        this.acc_id         = "";
        this.strand         = "";
        this.locDisplay     = "";
        this.objectType     = "";
        this.batchValue     = "";
    }

    /**
     * Returns a lucene document.
     *
     * @return Lucene document representing the data contained in this object.
     */
    protected Document prepareDocument() {

        doc.add(new Field(IndexConstants.COL_MARKER_SYMBOL, this.symbol,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_CHROMOSOME, this.chr,
                Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_MARKER_NAME, this.name,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_MARKER_TYPE, this.marker_type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_MGI_ID, this.acc_id,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_STRAND, this.strand,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_LOC_DISPLAY, this.locDisplay,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_OBJECT_TYPE, this.objectType,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_BATCH_FORWARD_VALUE, this.batchValue,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        return doc;
    }

    /**
     * Print out a string representation of this object.
     */
    public String toString() {
        return "Symbol: " + this.symbol
          + "DB Key: " + this.db_key
          + " Name: " + this.name
          + " Chromosome: " + this.chr
          + " Marker Type: " + this.marker_type
          + " Acc ID: " + this.acc_id
          + " Strand: " + this.strand;
    }

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

    /**
     * Get the value that will be passed ot the batch query form.
     * @return String
     */
    
    public String getBatchValue() {
        return batchValue;
    }

    /**
     * Set the value to be returned to the batch query form.
     * @param batchValue
     */
    
    public void setBatchValue(String batchValue) {
        if (batchValue != null) {
            this.batchValue = batchValue;            
        }
    }    
    
    /**
     * Get the accession id/
     * @return String representing the accession id.
     */

    public String getAcc_id() {
        return acc_id;
    }

    /**
     * Set the accession id.
     * @param acc_id String to set the accession id to.
     */

/*    public void setAcc_id(String acc_id) {
        if (acc_id != null) {
            this.acc_id = acc_id;
        }
        else {
            this.hasError = true;
        }
    }*/

    public void setAcc_id(String acc_id) {
        if (acc_id != null) {
            this.acc_id = acc_id;
        }
    }    
    
    public String getStrand() {
        return strand;
    }

    public void setStrand(String strand) {
        if (strand != null) {
            this.strand = strand;
        }
    }

    public String getLocDisplay() {
        return locDisplay;
    }

    public void setLocDisplay(String s) {
        if (s != null) {
            this.locDisplay = s;
        }
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }


    /**
     * This main program is a stub for a test harness that can be built to
     * specifically test this object.
     *
     * @param args Standard argument.
     */

    public static void main(String[] args) {
        // Set up the logger.

        GenomeFeatureDisplayLuceneDocBuilder builder =
            new GenomeFeatureDisplayLuceneDocBuilder();

        Logger log =
            Logger.getLogger(builder.getClass().getName());

        log.info(builder.getClass().getName() + " Test Harness");

        // Should result in an error being printed!, but the lucene document
        // should still come through.

        builder.setSymbol(null);
        Document doc = builder.getDocument();

        // Reset the doc builder for the next object.

        builder.clear();

        log.info("Lucene document: " + doc);

        // Should work properly, resulting in a Lucene document being returned.

        builder.setSymbol("test");
        builder.setDb_key("123");
        builder.setName("test type");
        builder.setChr("3");
        builder.setMarker_type("Test Type");
        builder.setAcc_id("testaccid");

        doc = builder.getDocument();

        // Should print out the toString() version of the doc builder.

        log.info(builder);

        log.info("Lucene document: " + doc);    }
}

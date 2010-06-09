package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Container Class that encapsulates the data needed to create
 * markerInexact index documents.
 *
 * @author mhall
 *
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into
 * a lucene document.
 *
 */

public class GenomeFeatureInexactLuceneDocBuilder extends AbstractLuceneDocBuilder {

    private String data_type    = "";
    private String isCurrent    = "1";
    private String organism     = "1";
    private String vocabulary   = "";
    private String raw_data     = "";
    private String display_type = "";
    private String unique_key   = "";

    /**
     * Resets the object back to its initial state. This allows us to reuse the
     * same object for our entire indexing process.
     */
    protected void clearLocal() {
        this.data_type      = "";
        this.isCurrent      = "1";
        this.organism       = "1";
        this.vocabulary     = "";
        this.raw_data       = "";
        this.display_type   = "";
        this.unique_key     = "";
        }

    /**
     * Returns a lucene document.
     *
     * This performs a special transformation, and replaces any non standard
     * whitespace character with a single blank space.
     *
     * @return A Lucene Document representing a MarkerAndVocabByField record.
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
        doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(IndexConstants.COL_IS_CURRENT,
                this.isCurrent, Field.Store.YES, Field.Index.NO));
        doc.add(new Field(IndexConstants.COL_ORGANISM, this.organism,
                Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field(IndexConstants.COL_OBJ_TYPE, this.vocabulary,
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
        return "DB Key: " + this.db_key + " Data: " + this.data
                + " Type: " + this.data_type + " Current: "
                + this.isCurrent + " Organism Type: " + this.organism
                + " Object/Vocabulary Type: " + this.vocabulary
                + " Raw Data: " + this.raw_data + " Unique Key: "
                + this.unique_key;
    }

    /**
     * Appends to the data field. Some of our fields are split up in the
     * database, so they have to be put back together manually at indexing
     * time.
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
            if (type.equals(IndexConstants.MP_DATABASE_TYPE)) {
                type = "MP";
            }
            if (type.equals(IndexConstants.INTERPRO_DATABASE_TYPE)) {
                type = "IP";
            }
            if (type.equals(IndexConstants.PIRSF_DATABASE_TYPE)) {
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
     * Sets the raw data field, this is used at display time to see an
     * unaltered form of the item we are matching on.
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
     * Append to the raw data field.
     * @param data
     */

    public void appendRaw_data(String data) {
        if (this.raw_data.equals("")) {
            this.raw_data = data;
        } else {
            this.raw_data = this.raw_data + data;
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

    /**
     * Returns the unique key for this document.  This key is used as a join
     * point across indexes at display time.
     *
     * @return String with this objects unique key.
     */

    public String getUnique_key() {
        return unique_key;
    }

    /**
     * Sets the unique key for this document.  This key is used as a join point
     * across indexes.
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
     * Test harness for this object.
     *
     * @param args
     */

    public static void main(String[] args) {
       // Set up the logger.

        GenomeFeatureInexactLuceneDocBuilder builder =
            new GenomeFeatureInexactLuceneDocBuilder();

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
        builder.setOrganism("test organism");
        builder.setVocabulary("MARKER");

        doc = builder.getDocument();

        // Should print out the toString() version of the doc builder.

        log.info(builder);

        log.info("Lucene document: " + doc);

    }
}


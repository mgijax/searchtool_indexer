package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data used in creating the VocabDisplay index.
 * 
 * @author mhall
 * 
 * @is A LuceneDocBuilder
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a lucene document.
 * 
 */
public class MarkerVocabDagLuceneDocBuilder implements LuceneDocBuilder {

    private String db_key       = "";
    private String contents     = "";
    private String gene_ids     = "";
    private String vocabulary   = "";
    private String child_ids    = "";
    private String acc_id       = "";
    private String type_display = "";
    private String unique_key   = "";
    
    private Logger log          = 
        Logger.getLogger(MarkerVocabDagLuceneDocBuilder.class.getName());
    
    private Boolean hasError    = false;

    /**
     * Returns the object to its default state.
     */

    public void clear() {
        this.gene_ids       = "";
        this.contents       = "";
        this.db_key         = "";
        this.vocabulary     = "";
        this.child_ids      = "";
        this.acc_id         = "";
        this.type_display   = "";
        this.unique_key     = "";
        this.hasError       = false;
    }

    /**
     * Returns a Lucene document.
     * 
     * @return Lucene document representing the data contained in this object.
     */

    public Document getDocument() {
        if (hasError) {
            log.error("Error while indexing: " +this.toString());
        }
        
        Document doc = new Document();

        doc.add(new Field(IndexConstants.COL_DB_KEY, this.getDb_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_VOCABULARY, this.getVocabulary(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        doc.add(new Field(IndexConstants.COL_CONTENTS, this.getContents(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_RAW_DATA, this.getContents(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.getTypeDisplay(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_GENE_IDS, this.getGene_ids(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_CHILD_IDS, this.getChild_ids(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.getUnique_key(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        
        return doc;
    }

    /**
     * Returns a string representation of the data contained within this object.
     */

    public String toString() {
        return "Contents: " + this.getContents() + " Database Key: "
                + this.getDb_key() + " Vocabulary: " + this.getVocabulary()
                + " Gene Ids: " + this.getGene_ids() + " Child Ids: "
                + this.getChild_ids() + " Accession Id: " + this.getAcc_id();
    }

    /**
     * This main program can be used to create a testHarness for this object.
     * 
     * @param args
     */
    public static void main(String[] args) {
       // Set up the logger.
        
        Logger log = Logger.getLogger(MarkerVocabDagLuceneDocBuilder.class.getName());
        
        log.info("MarkerVocabDagLuceneDocBuilder Test Harness");

        MarkerVocabDagLuceneDocBuilder meldb = new MarkerVocabDagLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        meldb.setContents(null);
        Document doc = meldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        meldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        meldb.setContents("test");
        meldb.setDb_key("123");        
        meldb.setUnique_key("123test_type");
        meldb.setVocabulary("MARKER");
        
        doc = meldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(meldb);
        
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
     * Set the database key
     * 
     * @param id
     */

    public void setDb_key(String id) {
        if (id != null) {
            this.db_key = id;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the contents (data) Should likely be refactored to the the data
     * field, much like many of the other indexes.
     * 
     * @return String representation of the contents field.
     */

    public String getContents() {
        return contents;
    }

    /**
     * Set the contents field.
     * 
     * @param contents
     */

    public void setContents(String contents) {
        if (contents != null) {
            this.contents = contents;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Append to the contents field, we have some items that span multiple 
     * rows in the database, this facilitates them.
     * @param contents
     */
    
    public void appendContents(String contents) {
        if (contents != null) {
            this.contents = this.contents + " " + contents;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Return the gene ids.
     * 
     * @return String representation of the gene_ids field.
     */

    public String getGene_ids() {
        return gene_ids;
    }

    /**
     * Set the gene_ids field.
     * 
     * @param gene_ids
     */

    public void setGene_ids(String gene_ids) {
        if (gene_ids != null) {
            this.gene_ids = gene_ids;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Add to the gene id field Add a single gene onto the list, commas are
     * inserted automatically.
     * 
     * @param gene_ids
     */

    public void appendGene_ids(String gene_ids) {
        if (gene_ids != null) {
            if (this.gene_ids.equals("")) {
                this.gene_ids = gene_ids;
            } else {
                this.gene_ids = this.gene_ids + "," + gene_ids;
            }
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
     * Set the vocabulary. There is special handling in here for when the data
     * passed is "Mammalian Phenotype" when we encounter that case we convert it
     * into "MP"
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
     * Returns the list of child ids. This list is put together at indexing
     * time, and travels down the dag where applicable.
     * 
     * @return String representation of the child_ids field
     */

    public String getChild_ids() {
        return child_ids;
    }

    /**
     * Set the child ids list.
     * 
     * @param child_ids
     * Comma delimited string
     */

    public void setChild_ids(String child_ids) {
        if (child_ids != null) {
            this.child_ids = child_ids;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Append a new child id onto the list Commas are inserted automatically.
     * 
     * @param child_ids
     */

    public void appendChild_ids(String child_ids) {
        if (child_ids != null) {
            if (this.child_ids.equals("")) {
                this.child_ids = child_ids;
            } else {
                this.child_ids = this.child_ids + "," + child_ids;
            }
        }
        else {
            this.hasError = true;
        }
    }


    /**
     * Returns the accession id
     * 
     * @return String representation of the acc_id field.
     */

    public String getAcc_id() {
        return acc_id;
    }

    /**
     * Sets the accession id.
     * 
     * @param acc_id
     * String
     */
    public void setAcc_id(String acc_id) {
        if (acc_id != null) {
            this.acc_id = acc_id;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the provider
     * 
     * @return String representation of the acc_id field.
     */

    public String getTypeDisplay() {
        return type_display;
    }

    /**
     * Sets the provider
     * 
     * @param type_display
     * String
     */
    public void setTypeDisplay(String type_display) {
        if (type_display != null) {
            this.type_display = type_display;
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

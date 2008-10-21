package org.jax.mgi.index.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import QS_Commons.IndexConstants;

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
public class VocabDisplayLuceneDocBuilder implements LuceneDocBuilder {

    private String db_key                 = "";
    private String contents               = "";
    private String gene_ids               = "";
    private String vocabulary             = "";
    private String marker_count           = "";
    private String annotation_count       = "0";
    private String annotation_objects     = "0";
    private String annotation_object_type = "0";
    private String child_ids              = "";
    private String acc_id                 = "";
    private String secondary_object_count = "0";
    private String type_display           = "";
    private Logger log                    = 
        Logger.getLogger(VocabDisplayLuceneDocBuilder.class.getName());
    
    private Boolean hasError              = false;

    /**
     * Returns the object to its default state.
     */

    public void clear() {
        this.gene_ids               = "";
        this.contents               = "";
        this.db_key                 = "";
        this.vocabulary             = "";
        this.marker_count           = "0";
        this.annotation_count       = "0";
        this.annotation_objects     = "0";
        this.annotation_object_type = "0";
        this.child_ids              = "";
        this.acc_id                 = "";
        this.secondary_object_count = "0";
        this.type_display           = "";
        this.hasError               = false;
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
        
        doc.add(new Field(IndexConstants.COL_GENE_IDS, this.getGene_ids(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_CHILD_IDS, this.getChild_ids(),
                Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_MARKER_COUNT,
                this.getMarker_count(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_ANNOT_COUNT,
                this.getAnnotation_count(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_ANNOT_OBJECTS,
                this.getAnnotation_objects(), Field.Store.YES,
                Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_ANNOT_OBJECT_TYPE,
                this.getAnnotation_object_type(), Field.Store.YES,
                Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_ANNOT_DISPLAY,
                this.getAnnot_display(), Field.Store.YES, 
                Field.Index.NO));
        
        doc.add(new Field("secondary_object_count",
                this.getSecondary_object_count(), Field.Store.YES,
                Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_ACC_ID,
                this.getAcc_id(), Field.Store.YES, Field.Index.NO));
        
        doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
                this.getTypeDisplay(), Field.Store.YES, Field.Index.NO));
        
        return doc;
    }

    /**
     * Returns a string representation of the data contained within this object.
     */

    public String toString() {
        return "Contents: " + this.getContents() + " Database Key: " 
            + this.getDb_key() + " Vocabulary: " + this.getVocabulary()
            + " Gene Ids: " + this.getGene_ids() + " Child Ids: " 
            + this.getChild_ids() + " Marker Count: " + this.getMarker_count()
            + " Annotation Count: " + this.getAnnotation_count()
            + " Annotation Objects: " + this.getAnnotation_objects() 
            + " Annotation Object Type: " + this.getAnnotation_object_type()
            + " Accession Id: " + this.getAcc_id();
    }

    /**
     * This main program can be used to create a testHarness for this object.
     * 
     * @param args
     */
    public static void main(String[] args) {
      // Set up the logger.
        
        Logger log =
            Logger.getLogger(VocabDisplayLuceneDocBuilder.class.getName());
        
        log.info("VocabDisplayLuceneDocBuilder Test Harness");

        VocabDisplayLuceneDocBuilder ldb = new VocabDisplayLuceneDocBuilder();
        
        // Should result in an error being printed!, but the lucene document
        // should still come through.
        
        ldb.setContents(null);
        Document doc = ldb.getDocument();
        
        // Reset the doc builder for the next object.
        
        ldb.clear();
        
        log.info("Lucene document: " + doc);
        
        // Should work properly, resulting in a Lucene document being returned.

        ldb.setContents("test");
        ldb.setDb_key("123");        
        ldb.setVocabulary("MARKER");
        ldb.setGene_ids("test gene ids");
        ldb.setMarker_count("test marker count");
        ldb.setAnnotation_count("2");
        ldb.setAnnotation_objects("3");
        ldb.setAnnotation_object_type("Marker");
        ldb.setChild_ids("213, 123123");
        ldb.setAcc_id("test12313245");
        ldb.setTypeDisplay("MARKER");
        
        doc = ldb.getDocument();

        // Should print out the toString() version of the doc builder.
        
        log.info(ldb);
        
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
     * passed is "Mammalian Phenotype" when we encounter that case we convert 
     * it into "MP"
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
     * Returns the marker count. This really should be removed in a future
     * version of this code, as its now calculated in the search cache.
     * 
     * @return String representation of the marker_count field.
     */
    public String getMarker_count() {
        return marker_count;
    }

    /**
     * Set the marker count
     * 
     * @param marker_count
     */

    public void setMarker_count(String marker_count) {
        if (marker_count != null) {
            this.marker_count = marker_count;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the annotation count.
     * 
     * @return String representation of the annotation count.
     */

    public String getAnnotation_count() {
        return annotation_count;
    }

    /**
     * Set the annotation count.
     * 
     * @param annotation_count
     */

    public void setAnnotation_count(String annotation_count) {
        if (annotation_count != null) {
            this.annotation_count = annotation_count;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Get the annotation object (count) This is probably miss named, and should
     * be refactored into something that makes more sense in the near future.
     * 
     * @return String representation of the annotation_objects field.
     */

    public String getAnnotation_objects() {
        return annotation_objects;
    }

    /**
     * Set the annotation objects (count)
     * 
     * @param annotation_objects
     */

    public void setAnnotation_objects(String annotation_objects) {
        if (annotation_objects != null) {
            this.annotation_objects = annotation_objects;
        }
        else {
            this.hasError = true;
        }
    }

    /**
     * Returns the annotation object type
     * 
     * @return String representation of the annotation_object_type field.
     */

    public String getAnnotation_object_type() {
        return annotation_object_type;
    }

    /**
     * Set the annotation object type.
     * 
     * @param annotation_object_type
     */

    public void setAnnotation_object_type(String annotation_object_type) {
        if (annotation_object_type != null) {
            this.annotation_object_type = annotation_object_type;
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
     * Get the secondary object count, some vocabularies have both the count of
     * annotations to the term, as well as the count of some other object, say
     * markers for example.
     */

    public String getSecondary_object_count() {
        return secondary_object_count;
    }

    /**
     * Set the secondary object count.
     * 
     * @param secondary_object_count
     */

    public void setSecondary_object_count(String secondary_object_count) {
        if (secondary_object_count != null) { 
            this.secondary_object_count = secondary_object_count;
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

    /**
     * Returns a realized string that will be used at display time. This string
     * contains the specific information for each vocabulary as to which details
     * they want displayed in their annotation section.
     * 
     * @return Realized String of the Annotation Display
     */

    public String getAnnot_display() {

        String annot_display = "";

        if (this.vocabulary.equals("OMIM")) {
            if (!this.annotation_count.equals("0") && !this.secondary_object_count.equals("0")) {
                annot_display = this.annotation_count + " mouse model";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }

                annot_display += ", " + this.secondary_object_count + " mouse ortholog";

                if (!this.secondary_object_count.equals("1")) {
                    annot_display += "s";
                }

            } else if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_count + " mouse model";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
            } else if (!this.secondary_object_count.equals("0")) {
                annot_display = this.secondary_object_count + " mouse ortholog";
                if (!this.secondary_object_count.equals("1")) {
                    annot_display += "s";
                }
            }

        } else if (this.vocabulary.equals("GO")) {
            if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_objects + " gene";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
                annot_display += ", " + this.annotation_count + " annotation";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
            }
        } else if (this.vocabulary.equals("IP")) {
            if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_objects + " gene";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
                annot_display += ", " + this.annotation_count + " annotation";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
            }
        } else if (this.vocabulary.equals("PS")) {
            if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_objects + " gene";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
            }
        } else if (this.vocabulary.equals("MP")) {
            if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_objects + " genotype";
                if (!this.annotation_objects.equals("1")) {
                    annot_display += "s";
                }
                annot_display += ", " + this.annotation_count + " annotation";
                if (!this.annotation_count.equals("1")) {
                    annot_display = annot_display + "s";
                }
            }
        } else if (this.vocabulary.equals("AD")) {
            if (!this.annotation_count.equals("0")) {
                annot_display = this.annotation_count + " gene expression result";
                if (!this.annotation_count.equals("1")) {
                    annot_display += "s";
                }
            }
        } else {
            annot_display = "Test";
        }

        return annot_display;
    }

}

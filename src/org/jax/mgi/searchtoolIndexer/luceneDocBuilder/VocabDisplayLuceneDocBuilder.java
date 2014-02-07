package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data used in creating the VocabDisplay index.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *       lucene document.
 * 
 */

public class VocabDisplayLuceneDocBuilder extends AbstractLuceneDocBuilder {

	private StringBuffer	gene_ids				= new StringBuffer("");
	private String			vocabulary				= "";
	private String			marker_count			= "";
	private String			annotation_count		= "0";
	private String			annotation_objects		= "0";
	private String			annotation_object_type	= "0";
	private StringBuffer	child_ids				= new StringBuffer("");
	private String			acc_id					= "";
	private String			secondary_object_count	= "0";
	private String			type_display			= "";

	/**
	 * Returns the object to its default state.
	 */

	protected void clearLocal() {
		gene_ids = new StringBuffer("");
		vocabulary = "";
		marker_count = "0";
		annotation_count = "0";
		annotation_objects = "0";
		annotation_object_type = "0";
		child_ids = new StringBuffer("");
		acc_id = "";
		secondary_object_count = "0";
		type_display = "";
	}

	/**
	 * Returns a Lucene document.
	 * 
	 * @return Lucene document representing the data contained in this object.
	 */

	protected Document prepareDocument() {

		doc.add(new Field(IndexConstants.COL_DB_KEY, db_key, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_VOCABULARY, vocabulary, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_CONTENTS, data.toString(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_GENE_IDS, gene_ids.toString(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_CHILD_IDS, child_ids.toString(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_MARKER_COUNT, marker_count, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_ANNOT_COUNT, annotation_count, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_ANNOT_OBJECTS, annotation_objects, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_ANNOT_OBJECT_TYPE, annotation_object_type, Field.Store.YES, Field.Index.NO));

		// This is very special in that its a realized field.

		doc.add(new Field(IndexConstants.COL_ANNOT_DISPLAY, getAnnot_display(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field("secondary_object_count", secondary_object_count, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_ACC_ID, acc_id, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY, type_display, Field.Store.YES, Field.Index.NO));

		return doc;
	}

	/**
	 * Returns a string representation of the data contained within this object.
	 */

	public String toString() {
		return "Data: " + data +
				" Database Key: " + db_key +
				" Vocabulary: " + vocabulary +
				" Gene Ids: " + gene_ids +
				" Child Ids: " + child_ids +
				" Marker Count: " + marker_count +
				" Annotation Count: " + annotation_count +
				" Annotation Objects: " + annotation_objects +
				" Annotation Object Type: " + annotation_object_type +
				" Accession Id: " + acc_id;
	}

	/**
	 * Append to the contents field, we have some items that span multiple rows
	 * in the database, this facilitates them.
	 * 
	 * @param contents
	 */

	// Might not be used, checking.

	/*
	 * public void appendContents(String data) { if (data != null) { this.data =
	 * this.data + " " + data; } else { this.hasError = true; } }
	 */

	/**
	 * Return the gene ids.
	 * 
	 * @return String representation of the gene_ids field.
	 */

	public String getGene_ids() {
		return gene_ids.toString();
	}

	/**
	 * Set the gene_ids field.
	 * 
	 * @param gene_ids
	 */

	public void setGene_ids(String gene_ids) {
		if (gene_ids != null) {
			this.gene_ids = new StringBuffer(gene_ids);
		}
		else {
			// System.out.println("Setting Error: setGene_ids: gene_ids: " +
			// gene_ids);
			this.hasError = true;
		}
	}

	/**
	 * Add to the gene id field
	 * 
	 * Add a single gene onto the list, commas are inserted automatically.
	 * 
	 * @param gene_ids
	 */

	public void appendGene_ids(String gene_ids) {
		if (gene_ids != null) {
			if (this.gene_ids.length() == 0) {
				this.gene_ids.append(gene_ids);
			} else {
				this.gene_ids.append(",").append(gene_ids);
			}
		}
		else {
			// System.out.println("Setting Error: appendGene_ids: gene_ids: " +
			// gene_ids);
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
	 * into "MP", similar conversions occur for "Interpro Domains" and
	 * "PIR Superfamily"
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
			// System.out.println("Setting Error: setVocabulary: type: " +
			// type);
			this.hasError = true;
		}
	}

	/**
	 * Returns the marker count. This can likely be removed in a future version
	 * of this code, as its now calculated in the search cache.
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
			// System.out.println("Setting Error: setMarker_count: marker_count: "
			// + marker_count);
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
	}

	/**
	 * Get the annotation object (count).
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
	}

	/**
	 * Returns the list of child ids. This list is put together at indexing
	 * time, and travels down the dag where applicable.
	 * 
	 * @return String representation of the child_ids field
	 */

	public String getChild_ids() {
		return child_ids.toString();
	}

	/**
	 * Set the child ids list.
	 * 
	 * @param child_ids
	 *            Comma delimited string
	 */

	public void setChild_ids(String child_ids) {
		if (child_ids != null) {
			this.child_ids = new StringBuffer(child_ids);
		}
		else {
			// System.out.println("Setting Error: setChild_ids: child_ids: " +
			// child_ids);
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
			if (this.child_ids.length() == 0) {
				this.child_ids.append(child_ids);
			} else {
				this.child_ids.append(",").append(child_ids);
			}
		}
		else {
			// System.out.println("Setting Error: appendChild_ids: child_ids: "
			// + child_ids);
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
			// System.out.println("Setting Error: setSecondary_object_count: secondary_object_count: "
			// + secondary_object_count);
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
	 *            String
	 */

	public void setAcc_id(String acc_id) {
		if (acc_id != null) {
			this.acc_id = acc_id;
		}
		else {
			// System.out.println("Setting Error: setAcc_id: acc_id: " +
			// acc_id);
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
	 *            String
	 */
	public void setTypeDisplay(String type_display) {
		if (type_display != null) {
			this.type_display = type_display;
		}
		else {
			// System.out.println("Setting Error: setTypeDisplay: type_display: "
			// + type_display);
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

		// If its omim, we will calculate the number of mouse models

		if (vocabulary.equals(IndexConstants.OMIM_TYPE_NAME)) {
			if (!annotation_count.equals("0") && !secondary_object_count.equals("0")) {
				annot_display = annotation_count + " mouse model";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}

				annot_display += ", " + secondary_object_count + " mouse ortholog";

				if (!secondary_object_count.equals("1")) {
					annot_display += "s";
				}

			} else if (!annotation_count.equals("0")) {
				annot_display = annotation_count + " mouse model";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
				// Secondly, how many orthologs do we have?

			} else if (!secondary_object_count.equals("0")) {
				annot_display = secondary_object_count + " mouse ortholog";
				if (!secondary_object_count.equals("1")) {
					annot_display += "s";
				}
			}

		} else if (vocabulary.equals(IndexConstants.GO_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_objects + " gene";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
				annot_display += ", " + annotation_count + " annotation";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.INTERPRO_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_objects + " gene";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
				annot_display += ", " + annotation_count + " annotation";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.PIRSF_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_objects + " gene";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.MP_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_objects + " genotype";
				if (!annotation_objects.equals("1")) {
					annot_display += "s";
				}
				annot_display += ", " + annotation_count + " annotation";
				if (!annotation_count.equals("1")) {
					annot_display = annot_display + "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.EMAPS_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_count + " gene expression result";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.EMAPA_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_count + " gene expression result";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else if (vocabulary.equals(IndexConstants.AD_TYPE_NAME)) {
			if (!annotation_count.equals("0")) {
				annot_display = annotation_count + " gene expression result";
				if (!annotation_count.equals("1")) {
					annot_display += "s";
				}
			}
		} else {
			annot_display = "ERROR INVALID VOCABULARY";
		}

		return annot_display;
	}

	/**
	 * Testharness for this object..
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Set up the logger.

		VocabDisplayLuceneDocBuilder builder =
				new VocabDisplayLuceneDocBuilder();

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
		builder.setVocabulary("MARKER");
		builder.setGene_ids("test gene ids");
		builder.setMarker_count("test marker count");
		builder.setAnnotation_count("2");
		builder.setAnnotation_objects("3");
		builder.setAnnotation_object_type("Marker");
		builder.setChild_ids("213, 123123");
		builder.setAcc_id("test12313245");
		builder.setTypeDisplay("MARKER");

		doc = builder.getDocument();

		// Should print out the toString() version of the doc builder.

		log.info(builder);

		log.info("Lucene document: " + doc);

	}

}

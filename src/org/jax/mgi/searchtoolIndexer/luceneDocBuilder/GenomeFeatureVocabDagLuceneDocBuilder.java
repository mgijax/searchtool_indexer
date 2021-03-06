package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the data used in creating the markerVocabDag index.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *       lucene document.
 * 
 */

public class GenomeFeatureVocabDagLuceneDocBuilder extends AbstractLuceneDocBuilder {

	private StringBuffer	gene_ids	= new StringBuffer("");
	private String			vocabulary	= "";
	private StringBuffer	child_ids	= new StringBuffer("");
	private String			acc_id		= "";
	private String			unique_key	= "";
	private String			object_type	= "";

	/**
	 * Returns the object to its default state.
	 */

	protected void clearLocal() {
		gene_ids = new StringBuffer("");
		vocabulary = "";
		child_ids = new StringBuffer("");
		acc_id = "";
		unique_key = "";
		object_type = "";
	}

	/**
	 * Returns a Lucene document.
	 * 
	 * @return Lucene document representing the data contained in this object.
	 */

	protected Document prepareDocument() {
		doc.add(new Field(IndexConstants.COL_DB_KEY, db_key, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_VOCABULARY, vocabulary, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_FEATURE_IDS, gene_ids.toString(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_CHILD_IDS, child_ids.toString(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, unique_key, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_OBJECT_TYPE, object_type, Field.Store.YES, Field.Index.UN_TOKENIZED));
		return doc;
	}

	/**
	 * Returns a string representation of the data contained within this object.
	 */

	public String toString() {
		return " Database Key: " + db_key + " Vocabulary: " + vocabulary + " Gene Ids: " + gene_ids + " Child Ids: " + child_ids + " Accession Id: " + acc_id;
	}

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
			if (this.gene_ids.length() == 0) {
				this.gene_ids.append(gene_ids);
			} else {
				this.gene_ids.append(",").append(gene_ids);
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
			this.hasError = true;
		}
	}

	/**
	 * Returns the unique key. This is used as a join point across indexes,
	 * 
	 * @return String with this objects unique key.
	 */

	public String getUnique_key() {
		return unique_key;
	}

	/**
	 * Sets the unique key. This is used as a join point across indexes.
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

	public String getObject_type() {
		return object_type;
	}

	public void setObject_type(String objectType) {
		object_type = objectType;
	}

	/**
	 * Testharness for this object.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Set up the logger.

		GenomeFeatureVocabDagLuceneDocBuilder builder =
				new GenomeFeatureVocabDagLuceneDocBuilder();

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
		builder.setUnique_key("123test_type");
		builder.setVocabulary("MARKER");

		doc = builder.getDocument();

		// Should print out the toString() version of the doc builder.

		log.info(builder);

		log.info("Lucene document: " + doc);

	}

}

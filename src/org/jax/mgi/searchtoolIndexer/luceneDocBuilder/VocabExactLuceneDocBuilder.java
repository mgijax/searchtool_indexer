package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

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
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *       lucene document.
 * 
 */

public class VocabExactLuceneDocBuilder extends AbstractLuceneDocBuilder {

	private String	data_type		= "";
	private String	vocabulary		= "";
	private String	raw_data		= "";
	private String	display_type	= "";
	private String	provider		= "";
	private String	unique_key		= "";

	/**
	 * Clears the object into its default state. This allows the object to be
	 * reused in the indexing process.
	 */
	protected void clearLocal() {
		this.data_type = "";
		this.vocabulary = "";
		this.raw_data = "";
		this.display_type = "";
		this.provider = "";
		this.unique_key = "";
	}

	/**
	 * Returns a Lucene document This object also performs so transformation on
	 * the data field, stripping out all unneeded whitespace before sending it
	 * off to be indexed.
	 * 
	 * @return Lucene document constructed with the information that this object
	 *         encapsulates.
	 */

	protected Document prepareDocument() {

		doc.add(new Field(IndexConstants.COL_DATA,
				this.data.toString().replaceAll("\\s+", " ")
						.replaceAll("^\\s", "").replaceAll("\\s$", "").toLowerCase(),
				Field.Store.YES, Field.Index.UN_TOKENIZED));

		doc.add(new Field(IndexConstants.COL_RAW_DATA, this.raw_data,
				Field.Store.YES, Field.Index.NO));

		doc.add(new Field(IndexConstants.COL_VOCABULARY, this.vocabulary,
				Field.Store.YES, Field.Index.UN_TOKENIZED));

		doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type,
				Field.Store.YES, Field.Index.UN_TOKENIZED));

		doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key,
				Field.Store.YES, Field.Index.UN_TOKENIZED));

		doc.add(new Field(IndexConstants.COL_TYPE_DISPLAY,
				this.display_type, Field.Store.YES, Field.Index.NO));

		doc.add(new Field(IndexConstants.COL_PROVIDER, this.provider,
				Field.Store.YES, Field.Index.NO));

		doc.add(new Field(IndexConstants.COL_UNIQUE_KEY, this.unique_key,
				Field.Store.YES, Field.Index.UN_TOKENIZED));

		return doc;
	}

	/**
	 * Returns a string representation of this objects data.
	 */

	public String toString() {
		return "Id: " + this.db_key + "  Raw Data: " + this.data
				+ "  Data Type: " + this.data_type + "  Vocabulary: "
				+ this.vocabulary + "  Unique Key: " + this.unique_key;
	}

	/**
	 * Returns the data type (Term Type)
	 * 
	 * @return String representation of the data_type field.
	 */

	public String getDataType() {
		return data_type;
	}

	/**
	 * Sets the data type (Term Type)
	 * 
	 * @param type
	 */

	public void setDataType(String type) {
		if (data_type != null) {
			this.data_type = type;
		}
		else {
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
	 * Returns the vocabulary.
	 * 
	 * @return String representation of the vocabulary field.
	 */

	public String getVocabulary() {
		return vocabulary;
	}

	/**
	 * Sets the vocabulary field. If the value passed to this function is
	 * "Mammalian Phenotype" then it transforms that data into "MP" for naming
	 * consistency in the indexes.
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
	 * Get the raw data
	 * 
	 * @return String
	 */

	public String getRaw_data() {
		return raw_data;
	}

	/**
	 * Set the raw data, this field does not get passed through an analyzer.
	 * 
	 * @param raw_data
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
	 * Sets the provider.
	 * 
	 * @return
	 */

	public String getProvider() {
		return provider;
	}

	/**
	 * Gets the provider.
	 * 
	 * @param provider
	 *            String
	 */

	public void setProvider(String provider) {
		if (provider != null) {
			this.provider = provider;
		}
		else {
			this.hasError = true;
		}
	}

	/**
	 * Returns the unique key for this document. This is used as a join point
	 * across indexes at display time.
	 * 
	 * @return String with this objects unique key.
	 */

	public String getUnique_key() {
		return unique_key;
	}

	/**
	 * Sets the unique key for this document. This is used as a join point
	 * across indexes at display time.
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
	 * The Main method is used as a test harness for this object.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Set up the logger.

		VocabExactLuceneDocBuilder builder = new VocabExactLuceneDocBuilder();

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
		builder.setVocabulary("MARKER");

		doc = builder.getDocument();

		// Should print out the toString() version of the doc builder.

		log.info(builder);

		log.info("Lucene document: " + doc);

	}
}

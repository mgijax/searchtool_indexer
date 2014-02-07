package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Object that encapsulates the information needed to create a single document
 * in the otherDisplay index.
 * 
 * @author mhall
 * 
 * @has Nothing
 * @does Knows how to take the data contained inside of it, and turn it into a
 *       Lucene document.
 * 
 */

public class OtherDisplayLuceneDocBuilder extends AbstractLuceneDocBuilder {

	private String	data_type	= "";
	private String	qualifier	= "";
	private String	name		= "";

	/**
	 * Clears the object into its default state. This allows the object to be
	 * reused in the indexing process.
	 */
	protected void clearLocal() {
		this.data_type = "";
		this.qualifier = "";
		this.name = "";
	}

	/**
	 * Returns a Lucene document.
	 * 
	 * @return Lucene document constructed with the information that this object
	 *         encapsulates.
	 */

	protected Document prepareDocument() {

		doc.add(new Field(IndexConstants.COL_DATA_TYPE, this.data_type, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_FEATURE_NAME, this.name, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(IndexConstants.COL_DB_KEY, this.db_key, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(IndexConstants.COL_QUALIFIER1, this.qualifier, Field.Store.YES, Field.Index.NO));
		return doc;
	}

	/**
	 * Returns a string representation of this objects data.
	 */

	public String toString() {
		return "Id: " + this.db_key + " Raw Data Type: "
				+ this.data_type + " Name: " + this.name
				+ " Qualifier: " + this.qualifier;

	}

	/**
	 * Returns the data type field.
	 * 
	 * @return String
	 */

	public String getDataType() {
		return data_type;
	}

	/**
	 * Sets the data type
	 * 
	 * @param data
	 *            String
	 */

	public void setDataType(String data) {
		if (data != null) {
			this.data_type = data;
		}
		else {
			this.hasError = true;
		}
	}

	/**
	 * Gets the first qualifier This is qualifier to set the sub type of an
	 * object.
	 * 
	 * @return String
	 */

	public String getQualifier() {
		return qualifier;
	}

	/**
	 * Sets the first qualifier string. This is the qualifier that shows the sub
	 * type of the object.
	 * 
	 * @param q
	 */

	public void setQualifier(String q) {

		if (q != null) {
			this.qualifier = q;
		}
		else {
			this.hasError = true;
		}
	}

	/**
	 * Returns the name of the object
	 * 
	 * @return String
	 */

	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the object.
	 * 
	 * @param name
	 */

	public void setName(String name) {
		if (name != null) {
			this.name = name;
		} else {
			this.hasError = true;
		}
	}

	/**
	 * Test harness for thie object.
	 * 
	 * @param args
	 */

	public static void main(String[] args) {
		// Set up the logger.

		OtherDisplayLuceneDocBuilder builder =
				new OtherDisplayLuceneDocBuilder();

		Logger log =
				Logger.getLogger(builder.getClass().getName());

		log.info(builder.getClass().getName() + " Test Harness");

		// Should result in an error being printed!, but the lucene document
		// should still come through.

		builder.setName(null);
		Document doc = builder.getDocument();

		// Reset the doc builder for the next object.

		builder.clear();

		log.info("Lucene document: " + doc);

		// Should work properly, resulting in a Lucene document being returned.

		builder.setName("test");
		builder.setDb_key("123");
		builder.setDataType("123test_type");
		builder.setQualifier("MARKER");

		doc = builder.getDocument();

		// Should print out the toString() version of the doc builder.

		log.info(builder);

		log.info("Lucene document: " + doc);

	}
}

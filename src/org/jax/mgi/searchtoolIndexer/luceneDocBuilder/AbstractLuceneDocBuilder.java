package org.jax.mgi.searchtoolIndexer.luceneDocBuilder;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * This class is the base class that all LuceneDocBuilders share.
 * 
 * @author mhall
 * 
 * @has nothing
 * @does defines a template for all LuceneDocBuilders to implement.
 */

public abstract class AbstractLuceneDocBuilder {

	protected Logger		log			=
												Logger.getLogger(this.getClass().getName());

	// The hasError flag is used on all of the doc builders.
	protected Boolean		hasError	= false;

	// Each doc builder, needs a Lucene Document.
	protected Document		doc;

	// Common Fields (Shared in > 90% of the indexes)

	protected String		db_key		= "";
	protected StringBuffer	data		= new StringBuffer("");

	/**
	 * Common clear method, which calls a locally defined clearLocal method.
	 * This uses the Template Method Pattern.
	 */

	public void clear() {
		this.db_key = "";
		this.data = new StringBuffer("");
		this.hasError = false;
		clearLocal();
	}

	/**
	 * This template method must be implemented in all descendant classes. This
	 * is where they will clear thier locally defined variables for reuse.
	 */

	protected abstract void clearLocal();

	/**
	 * Common getDocument method, which calls the locally defined
	 * preparedDocument method. This uses the Template Method Pattern.
	 * 
	 * @return A Lucene Document
	 */

	public Document getDocument() {
		checkErrors();
		doc = new Document();
		return prepareDocument();
	}

	/**
	 * Logs an error message is the boolean hasError flag has been set.
	 */

	public void checkErrors() {
		if (hasError) {
			log.error("Error while indexing: " + toString());
		}
	}

	/**
	 * All concrete implementations of this class, must overwrite this method.
	 * Its intent is to encapsulate the local process of building a Lucene
	 * Document.
	 * 
	 * @return A Lucene Document
	 */

	protected abstract Document prepareDocument();

	// Common Field Getters and Setters

	/**
	 * Returns the database key.
	 * 
	 * @return String representation of the database key.
	 */

	public String getDb_key() {
		return db_key;
	}

	/**
	 * Sets the database key. Defaults to an empty string.
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
	 * Returns the data field.
	 * 
	 * @return String representation of the data to be indexed.
	 */

	public String getData() {
		return data.toString();
	}

	/**
	 * Sets the data field.
	 * 
	 * @param data
	 */

	public void setData(String data) {
		if (data != null) {
			this.data = new StringBuffer(data);
		}
		else {
			// System.out.println("Setting Error: setData: " + data);
			this.hasError = true;
		}
	}

	/**
	 * Appends data to the field to be searched. Since the data in the database
	 * could be compound in nature, you have to have the ability to put this
	 * field together.
	 * 
	 * @param data
	 */

	public void appendData(String data) {
		if (data != null) {
			if (this.data.length() == 0) {
				this.data.append(data);
			} else {
				this.data.append(data);
			}
		}
		else {
			this.hasError = true;
		}
	}

}

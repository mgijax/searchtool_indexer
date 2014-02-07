package org.jax.mgi.searchtoolIndexer.gatherer;

public class VocabSpec {

	public VocabSpec() {
	}

	private String	voc_key;
	private String	display_key;
	private String	dag_key	= null;
	private String	object_type;

	public String getVoc_key() {
		return voc_key;
	}

	public void setVoc_key(String vocKey) {
		voc_key = vocKey;
	}

	public String getDisplay_key() {
		return display_key;
	}

	public void setDisplay_key(String displayKey) {
		display_key = displayKey;
	}

	public String getDag_key() {
		return dag_key;
	}

	public void setDag_key(String dagKey) {
		dag_key = dagKey;
	}

	public String getObject_type() {
		return object_type;
	}

	public void setObject_type(String objectType) {
		object_type = objectType;
	}

}

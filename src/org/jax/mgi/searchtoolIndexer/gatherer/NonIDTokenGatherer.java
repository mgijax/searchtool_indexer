package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.NonIDTokenLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * This class is responsible for gathering up every unique token that we have in
 * all of our various datasources. It basically replicates the searches
 * performed in both markers and vocabulary with a slightly different treatment
 * of the words themselves.
 * 
 * This information is then used to build the nonIDToken index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started it runs through a group of methods that gather up
 *       all of the different items we want searched in this index.
 * 
 *       Each method behaves as follows:
 * 
 *       Gather its data, parse it, add the document to the stack, repeat until
 *       finished, clean up the result set, and exit.
 * 
 *       After each method has completed, we clean up the overall jdbc
 *       connection, set gathering to complete and exit.
 * 
 */

public class NonIDTokenGatherer extends DatabaseGatherer {

	// Class Variables
	// Instantiate the single doc builder that this object will use.

	private NonIDTokenLuceneDocBuilder	builder	= new NonIDTokenLuceneDocBuilder();

	public NonIDTokenGatherer(IndexCfg config) {
		super(config);
	}

	public void runLocal() throws Exception {
		doMarkerLabels();
		doVocabTerm();
		doVocabSynonym();
		doVocabNotes();
		doAlleleSynonym();
	}

	/**
	 * Gather the marker labels.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doMarkerLabels() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up marker key, label, label type, organism key, label status
		// and label type name where the marker is not withdrawn and the
		// organism is mouse.

		String GEN_MARKER_LABEL = "select ml._Marker_key, ml.label, "
				+ "ml.labelType, ml._OrthologOrganism_key, "
				+ "ml._Label_Status_key, ml.labelTypeName"
				+ " from MRK_Label ml, MRK_Marker m"
				+ " where ml._Organism_key = 1 and ml._Marker_key = "
				+ "m._Marker_key and m._Marker_Status_key = 1 ";

		// Gather the data

		ResultSet rs = executor.executeMGD(GEN_MARKER_LABEL);

		log.info("Time taken gather marker label result set "
				+ executor.getTiming());

		// Parse it

		while (rs.next()) {

			builder.setData(rs.getString("label"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());

			if (rs.getString("labelType").equals("AS")) {
				builder.setData(rs.getString("label").replaceAll("<", "").replaceAll(">", ""));
				documentStore.push(builder.getDocument());
			}
			builder.clear();
		}

		// Clean up

		rs.close();
		log.info("Done Marker Labels!");

	}

	/**
	 * Gather Vocab terms, non AD.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerm() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the vocabulary terms in all vocabularies but AD
		// This includes GO, MP, PIRSF, Interpro and DO (Disease Ontology).

		String VOC_TERM_KEY = "select _Term_key, term, vocabName"
				+ " from VOC_Term_View"
				+ " where isObsolete != 1 and _Vocab_key in (125, 4, 5, 8, 46, 90, 91)";

		// Gather the data

		ResultSet rs_term = executor.executeMGD(VOC_TERM_KEY);

		log.info("Time taken gather vocab term result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_term.next()) {

			builder.setData(rs_term.getString("term"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		log.info("Done Vocab Terms!");
		rs_term.close();

	}

	/**
	 * Gather the vocab synonyms, non AD.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonym() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the synonyms for all the vocabularies outside of AD.
		// This list currently includes GO, MP, Disease Ontology (DO), PIRSF, InterPro, EMAPA and
		// EMAPS

		String VOC_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName"
				+ " from VOC_Term_View tv, MGI_Synonym s"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90, 91)"
				+ " and s._MGIType_key = 13 ";

		// Gather the data

		ResultSet rs_syn = executor.executeMGD(VOC_SYN_KEY);

		log.info("Time taken gather vocab synonym result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_syn.next()) {

			builder.setData(rs_syn.getString("synonym"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		log.info("Done Vocab Synonyms!");
		rs_syn.close();
	}

	/**
	 * Gather the vocab notes/definitions, non AD.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNotes() throws SQLException, InterruptedException {

		// SQL for this subsection, please note that the order by clause is
		// important for this sql statement.

		// Select vocab notes for the non ad vocabularies, for non obsolete
		// terms.
		// This list currently includes GO, MP, PIRSF, InterPro, Disease Ontology (DO), EMAPA,
		// and EMAPS

		String VOC_NOTE_KEY = "select tv._Term_key, tv.note, tv.vocabName "
				+ " from VOC_Term_View tv "
				+ " where tv.note is not null"
				+ " and tv.isObsolete != 1 "
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90, 91)"
				+ " order by tv._Term_key";

		// Gather the data

		ResultSet rs_note = executor.executeMGD(VOC_NOTE_KEY);

		log.info("Time taken gather vocab notes/definition result set: "
				+ executor.getTiming());

		// Parse it

		int place = -1;

		// Since notes are compound rows in the database, we have to
		// contruct the searchable field.

		while (rs_note.next()) {
			if (place != rs_note.getInt(1)) {
				if (place != -1) {

					// Place the document on the stack.

					documentStore.push(builder.getDocument());
					builder.clear();
				}
				place = rs_note.getInt("_Term_key");
			}
			builder.appendData(rs_note.getString("note"));
		}

		log.info("Done Vocab Notes/Definitions!");
	}

	/**
	 * Gather the marker labels.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAlleleSynonym()
			throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up allele synonyms who's label is still in a current
		// status.
		// Please note that this sql should likely be changed to be using
		// all_allele instead of gxd_allelegenotype as per tr 9502

		String ALLELE_SYNONYM_KEY = "select distinct gag._Marker_key,"
				+ " al.label, al.labelType, al.labelTypeName"
				+ " from all_label al, GXD_AlleleGenotype gag"
				+ " where al.labelType = 'AY' and al._Allele_key ="
				+ " gag._Allele_key and al._Label_Status_key != 0";

		// Gather the data

		ResultSet rs = executor.executeMGD(ALLELE_SYNONYM_KEY);

		log.info("Time taken gather allele synonym result set "
				+ executor.getTiming());

		// Parse it

		while (rs.next()) {

			builder.setData(rs.getString("label"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());

			// Place another copy of the label w/o the allele markups.

			builder.setData(rs.getString("label").replaceAll("<", "").replaceAll(">", ""));
			documentStore.push(builder.getDocument());

			builder.clear();
		}

		// Clean up

		rs.close();
		log.info("Done Marker Labels!");

	}
}

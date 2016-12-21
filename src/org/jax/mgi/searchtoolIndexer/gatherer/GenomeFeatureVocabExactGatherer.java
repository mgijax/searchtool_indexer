package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureVocabExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up the vocabulary term information
 * that is used when returning items to the marker bucket. We specifically
 * exclude vocabulary terms that have no annotations to markers.
 * 
 * This information is then used to populate the markerVocabExact index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 *       Each component basically makes a call to the database and then starts
 *       parsing through its result set. For each record, we generate a Lucene
 *       document, and place it on the shared stack.
 * 
 *       After all of the components are finished, we notify the stack that
 *       gathering is complete, clean up our jdbc connections and exit.
 */

public class GenomeFeatureVocabExactGatherer extends DatabaseGatherer {

	// Class Variables

	private GenomeFeatureVocabExactLuceneDocBuilder	builder		=
																		new GenomeFeatureVocabExactLuceneDocBuilder();

	private HashMap<String, String>					providerMap	= new HashMap<String, String>();

	public GenomeFeatureVocabExactGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.VOCAB_TERM, "Term");
		providerMap.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
		providerMap.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
		providerMap.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
		providerMap.put(IndexConstants.DO_ORTH_TYPE_NAME, "Disease Ortholog");
		providerMap.put(IndexConstants.DO_DATABASE_TYPE, "Disease Model");
		providerMap.put(IndexConstants.GO_TYPE_NAME, "Function");
		providerMap.put(IndexConstants.AD_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPA_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPS_TYPE_NAME, "Expression");
	}

	public void runLocal() throws Exception {
		doVocabTerms();
		doVocabSynonyms();
		doVocabNotes();
	}

	/**
	 * Gather the Vocab Terms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerms() throws SQLException, InterruptedException {

		// SQL for this Subsection

//		log.info("Collecting EMAPA terms!");
//
//		// Collect emapa terms that are related to markers and are not obsolete.
//		String EMAPA_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
//				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
//				+ " where tv.isObsolete != 1 and tv._Vocab_key = 90"
//				+ " and tv._Term_key = vacc._Term_key and"
//				+ " vacc.annotType = 'EMAPA'";
//
//		doVocabTerm(EMAPA_TERM_KEY);

		log.info("Collecting EMAPS terms!");

		// Collect emaps terms that are related to markers and are not obsolete.
		String EMAPS_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 91"
				+ " and tv._Term_key = vacc._Term_key and"
				+ " vacc.annotType = 'EMAPS'";

		doVocabTerm(EMAPS_TERM_KEY, "EMAPS");

		log.info("Collecting GO terms!");

		// Collect go terms that are related to markers and are not obsolete.

		String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 4"
				+ " and tv._Term_key = vacc._Term_key and"
				+ " vacc.annotType = 'GO/Marker'";

		doVocabTerm(GO_TERM_KEY, "GO");

		log.info("Collecting MP Terms");

		// Collect mp terms that are related to markers and are not obsolete.

		String MP_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 5"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'Mammalian Phenotype/Genotype'";

		doVocabTerm(MP_TERM_KEY, "MP");

		log.info("Collecting Interpro Terms");

		String INTERPRO_TERM_KEY = "select tv._Term_key, tv.term,"
				+ " tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 8"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'InterPro/Marker'";

		doVocabTerm(INTERPRO_TERM_KEY, "InterPro");

		log.info("Collecting PIRSF Terms");

		// Collect pirsf terms that are related to markers and are not
		// obsolete.

		String PIRSF_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 46"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'PIRSF/Marker'";

		doVocabTerm(PIRSF_TERM_KEY, "PIRSF");

		log.info("Collecting DO Terms");

		// Collect Disease Ontology (DO)/non-human terms that are related to markers and are
		// not obsolete.

		String DO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 125"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'DO/Genotype'";

		doVocabTerm(DO_TERM_KEY, "DO/Mouse");

		log.info("Collecting DO/Human Terms");

		// Collect DO/human terms that are related to markers and are not
		// obsolete.

		String DO_HUMAN_TERM_KEY = "select tv._Term_key, tv.term," +
				" '" + IndexConstants.DO_ORTH_TYPE_NAME + "' as vocabName" +
				" from VOC_Term_View tv, VOC_Annot_Count_Cache vacc" +
				" where tv.isObsolete != 1 and tv._Vocab_key = 125" +
				" and tv._Term_key = vacc._Term_key and vacc.annotType" +
				" = 'DO/Human Marker'";

		doVocabTerm(DO_HUMAN_TERM_KEY, "DO/Human");

		log.info("Done collecting All Vocab Terms!");

	}

	/**
	 * Gather the vocab synonyms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonyms() throws SQLException, InterruptedException {

		// SQL for this Subsection

//		log.info("Collecting EMAPA Synonyms");
//
//		// Collect go synonyms that are related to markers, and are not
//		// obsolete.
//
//		String EMAPA_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
//				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
//				+ " Voc_Annot_count_cache vacc"
//				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
//				+ " and tv._Vocab_key = 90 and s._MGIType_key = 13 "
//				+ "and tv._Term_key = vacc._Term_key and vacc.annotType"
//				+ " = 'EMAPA'";
//
//		doVocabSynonym(EMAPA_SYN_KEY);

		log.info("Collecting EMAPS Synonyms");

		// Collect go synonyms that are related to markers, and are not
		// obsolete.

		String EMAPS_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 91 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType"
				+ " = 'EMAPS'";

		doVocabSynonym(EMAPS_SYN_KEY, "EMAPS");

		log.info("Collecting GO Synonyms");

		// Collect go synonyms that are related to markers, and are not
		// obsolete.

		String GO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 4 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType"
				+ " = 'GO/Marker'";

		doVocabSynonym(GO_SYN_KEY, "GO");

		log.info("Collecting MP Synonyms");

		// Collect mp synonyms that are related to markers, and are not
		// obsolete.

		String MP_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 5 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType"
				+ " = 'Mammalian Phenotype/Genotype'";

		doVocabSynonym(MP_SYN_KEY, "MP");

		log.info("Collecting DO Synonyms");

		// Collect DO/non-human synonyms that are related to markers and are
		// not obsolete.

		String DO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 125 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'DO/Genotype'";

		doVocabSynonym(DO_SYN_KEY, "DO/Mouse");

		log.info("Collecting DO/Human Synonyms");

		// Collect DO/human synonyms that are related to markers and are not
		// obsolete.

		String DO_HUMAN_SYN_KEY = "select tv._Term_key, s.synonym, '"
				+ IndexConstants.DO_ORTH_TYPE_NAME + "' as vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 125 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'DO/Human Marker'";

		doVocabSynonym(DO_HUMAN_SYN_KEY, "DO/Human");

		log.info("Done collecting All Vocab Synonyms!");

	}

	/**
	 * Gather the vocab notes/definitions. Please note that this is a
	 * compound field, and thus its parser has special handling.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNotes() throws SQLException, InterruptedException {

		// SQL for this subsection

		log.info("Collecting GO Notes/Definitions");

		// Collect go notes that are related to markers and are not obsolete.
		// These are ordered by sequence number so they can be put back together
		// in the lucene document

		String GO_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
				+ " from VOC_Term_View tv, VOC_text t,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
				+ " and tv._Vocab_key = 4"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'GO/Marker' order by tv._Term_key, t.sequenceNum";

		doVocabNote(GO_NOTE_KEY, "GO");

		log.info("Collecting MP Notes/Definitions");

		// Collect mp notes that are related to markers and are not obsolete.
		// These are ordered by sequence number so they can be put back together
		// in the lucene document

		String MP_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
				+ " from VOC_Term_View tv, VOC_text t,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
				+ " and tv._Vocab_key = 5"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'Mammalian Phenotype/Genotype'"
				+ " order by tv._Term_key, t.sequenceNum";

		doVocabNote(MP_NOTE_KEY, "MP");

		log.info("Collecting DO Notes/Definitions");

		// Collect DO (Disease Ontology) notes that are related to markers and are not obsolete.
		// These are ordered by sequence number so they can be put back together
		// in the lucene document

		String DO_NOTE_KEY = "select distinct tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
				+ " from VOC_Term_View tv, VOC_Text t, VOC_Annot_Count_Cache vacc"
				+ " where tv._Term_key = t._Term_key "
				+ " and tv.isObsolete != 1 "
				+ " and tv._Vocab_key = 125 "
				+ " and tv._Term_key = vacc._Term_key "
				+ " and vacc.annotType in ('DO/Genotype', 'DO/Human Marker') "
				+ " order by tv._Term_key, t.sequenceNum";

		//doVocabNote(DO_NOTE_KEY, "DO");

		log.info("Done collecting all Vocab Notes/Definitions");

	}

	/**
	 * Gather the Vocab Terms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerm(String sql, String vocab) throws SQLException, InterruptedException {

		ResultSet rs_term = executor.executeMGD(sql);
		rs_term.next();

		log.debug("Time taken gather " + vocab + " term result set: " + executor.getTiming());

		// Parse it

		int count = 0;
		while (!rs_term.isAfterLast()) {
			count++;
			builder.setVocabulary(rs_term.getString("vocabName"));
			builder.setData(rs_term.getString("term"));
			builder.setRaw_data(rs_term.getString("term"));
			builder.setDb_key(rs_term.getString("_Term_key"));
			builder.setDataType(IndexConstants.VOCAB_TERM);
			builder.setUnique_key(rs_term.getString("_Term_key") + rs_term.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_term.getString("vocabName")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_term.next();
		}

		// Clean up

		rs_term.close();
		log.info("Processed " + count + " " + vocab + " terms"); 
	}

	/**
	 * Gather the vocab synonyms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonym(String sql, String vocab)
			throws SQLException, InterruptedException {

		// Gather the Data

		ResultSet rs_syn = executor.executeMGD(sql);
		rs_syn.next();

		log.debug("Time taken gather " + vocab + " synonym result set: " + executor.getTiming());

		// Parse it

		int count = 0;
		while (!rs_syn.isAfterLast()) {
			count++;
			builder.setData(rs_syn.getString("synonym"));
			builder.setRaw_data(rs_syn.getString("synonym"));
			builder.setDb_key(rs_syn.getString("_Term_key"));
			builder.setVocabulary(rs_syn.getString("vocabName"));
			builder.setDataType(IndexConstants.VOCAB_SYNONYM);
			builder.setUnique_key(rs_syn.getString("_Synonym_key") + IndexConstants.VOCAB_SYNONYM + rs_syn.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_syn.getString("vocabName")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_syn.next();
		}

		// Clean up

		rs_syn.close();
		log.info("Processed " + count + " " + vocab + " synonyms");
	}

	/**
	 * Gather the vocab notes/definitions. Please note that this is a
	 * compound field, and thus its parser has special handling.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNote(String sql, String vocab)
			throws SQLException, InterruptedException {

		// Gather the data.

		ResultSet rs_note = executor.executeMGD(sql);
		rs_note.next();

		log.debug("Time taken gather " + vocab + " note result set: " + executor.getTiming());

		// Parse it

		int place = -1;
		int count = 0;

		while (!rs_note.isAfterLast()) {
			count++;
			if (place != rs_note.getInt("_Term_key")) {
				if (place != -1) {
					builder.setRaw_data(builder.getData());

					// Place the document on the stack.

					documentStore.push(builder.getDocument());
					builder.clear();
				}

				builder.setDb_key(rs_note.getString("_Term_key"));
				builder.setVocabulary(rs_note.getString("vocabName"));
				builder.setDataType(IndexConstants.VOCAB_NOTE);
				builder.setUnique_key(rs_note.getString("_Term_key") + IndexConstants.VOCAB_NOTE + rs_note.getString("vocabName"));
				builder.setDisplay_type(providerMap.get(rs_note.getString("vocabName")));
				place = rs_note.getInt("_Term_key");
			}
			builder.appendData(rs_note.getString("note"));
			rs_note.next();
		}

		// Clean up

		rs_note.close();
		log.info("Processed " + count + " " + vocab + " notes");
	}
}

package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureVocabAccIDLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we need to
 * generate the markerAccID index.
 * 
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

public class GenomeFeatureVocabAccIDGatherer extends DatabaseGatherer {

	// Class Variables

	private GenomeFeatureVocabAccIDLuceneDocBuilder builder = new GenomeFeatureVocabAccIDLuceneDocBuilder();

	private HashMap<String, String> providerMap = new HashMap<String, String>();

	public GenomeFeatureVocabAccIDGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.ACCESSION_ID, "ID");

		/*
		 * Vocab Providers are special, so we will use the HashMap to take take
		 * care of this relationship.
		 */

		providerMap.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
		providerMap.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
		providerMap.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
		providerMap.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
		providerMap.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
		providerMap.put(IndexConstants.GO_TYPE_NAME, "Function");
		providerMap.put(IndexConstants.EMAPA_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPS_TYPE_NAME, "Expression");

	}

	public void runLocal() throws Exception {
		doVocabAccessionIDs();
	}

	/**
	 * Gather the vocabulary accession ID's. Please note that AD has no
	 * accession ID's
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabAccessionIDs() throws SQLException,
			InterruptedException {

		// SQL for this Subsection
		// As long as the column names remain the same, the SQL for any
		// given vocabulary can move independently of each other.

		log.info("Collecting EMAPS Accession ID's");

		// Gather up the emaps accession ID's, ignoring ID's that are obsolete.

		String EMAPS_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
				+ " tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 91"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'EMAPS'";

		doVocabAccessionID(EMAPS_ACCID_KEY);

//		log.info("Collecting EMAPA Accession ID's");
//
//		// Gather up the emapa accession ID's, ignoring ID's that are obsolete.
//
//		String EMAPA_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
//				+ " tv.term"
//				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
//				+ " where isObsolete != 1 and _Vocab_key = 90"
//				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
//				+ " 'EMAPA'";
//
//		doVocabAccessionID(EMAPA_ACCID_KEY);

		log.info("Collecting GO Accession ID's");

		// Gather up the go accession ID's, ignoring ID's that are obsolete.

		String GO_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
				+ " tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 4"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'GO/Marker'"
				+ "union "
				+ "select vtv._Term_key, a.accId, 'GO' as vocabName, vtv.term "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 4 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0";
				;

		doVocabAccessionID(GO_ACCID_KEY);

		log.info("Collecting MP Accession ID's");

		// Gather up the mp accession ID's ignorning ID's that are obsolete.

		String MP_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName, tv.term "
				+ "from VOC_Term_View tv, VOC_Annot_Count_Cache vacc "
				+ "where isObsolete != 1 and _Vocab_key = 5 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType = "
				+ "'Mammalian Phenotype/Genotype' "
				+ "union "
				+ "select vtv._Term_key, a.accId, 'Mammalian Phenotype' as vocabName, vtv.term "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 5 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0";

		doVocabAccessionID(MP_ACCID_KEY);

		log.info("Collecting Interpro Accession ID's");

		// Gather up the interpro accession ID's ignoring ID's that are
		// obsolete.

		String INTERPRO_ACCID_KEY = "select tv._Term_key, tv.accId,"
				+ " tv.vocabName, tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 8"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'InterPro/Marker'";

		doVocabAccessionID(INTERPRO_ACCID_KEY);

		log.info("Collecting PIRSF Accession ID's");

		// Gather up pirsf accession ID's ignoring ID's that are obsolete.

		String PIRSF_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
				+ " tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 46"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'PIRSF/Marker'";

		doVocabAccessionID(PIRSF_ACCID_KEY);

		log.info("Collecting OMIM Accession ID's");

		// Gather up omim non human accession ID's, ignoring ID's that are
		// obsolete.

		String OMIM_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
				+ " tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 44"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'OMIM/Genotype'";

		doVocabAccessionID(OMIM_ACCID_KEY);

		log.info("Collecting OMIM/Human Accession ID's");

		// Gather up omim human accession ID's, ignorning ID's that are
		// obsolete.

		String OMIM_HUMAN_ACCID_KEY = "select tv._Term_key, tv.accId," + " '"
				+ IndexConstants.OMIM_ORTH_TYPE_NAME + "' as vocabName,"
				+ " tv.term"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where isObsolete != 1 and _Vocab_key = 44"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'OMIM/Human Marker'";

		doVocabAccessionID(OMIM_HUMAN_ACCID_KEY);

		log.info("Done collecting All Vocab Accession IDs!");

	}

	/**
	 * Generic Accession ID Gathering Algorithm. It takes a SQL string with an
	 * assumed format, and creates Lucene Documents from it.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabAccessionID(String sql) throws SQLException,
			InterruptedException {

		// Gather the data

		ResultSet rs_acc_id = executor.executeMGD(sql);
		rs_acc_id.next();

		log.info("Time taken gather result set: " + executor.getTiming());

		// Parse it

		while (!rs_acc_id.isAfterLast()) {
			builder.setData(rs_acc_id.getString("accId"));
			builder.setRaw_data(rs_acc_id.getString("term"));
			builder.setDb_key(rs_acc_id.getString("_Term_key"));
			builder.setVocabulary(rs_acc_id.getString("vocabName"));
			builder.setDataType(IndexConstants.VOC_ACCESSION_ID);
			builder.setDisplay_type(providerMap.get(rs_acc_id.getString("vocabName")));
			builder.setProvider("(" + rs_acc_id.getString("accId") + ")");

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_acc_id.next();
		}

		// Clean up

		rs_acc_id.close();

	}
}

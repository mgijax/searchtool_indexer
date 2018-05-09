package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.VocabAccIDLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.StrainUtils;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Gather up all of the information needed to search for vocabulary terms by
 * accession id.
 * 
 * This information is then used to populate the vocabAccID index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started this runs through a group of methods, each of which
 *       are responsible for gathering documents from a different accession id
 *       type.
 * 
 *       Each subprocess basically operates as follows:
 * 
 *       Gather the data for the specific sub-type, parse it while creating
 *       lucene documents and adding them to the stack.
 * 
 *       After it completes parsing, it cleans up its result sets, and exits.
 * 
 *       After all of these methods complete, we set gathering complete to true
 *       in the shared document stack and exit.
 */

public class VocabAccIDGatherer extends DatabaseGatherer {

	// Class Variables

	private VocabAccIDLuceneDocBuilder	builder		=
															new VocabAccIDLuceneDocBuilder();

	private HashMap<String, String>		providerMap	= new HashMap<String, String>();

	public VocabAccIDGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.VOCAB_TERM, "Term");
		providerMap.put(IndexConstants.VOCAB_SYNONYM, "Term Synonym");
		providerMap.put(IndexConstants.VOCAB_NOTE, "Term Definition");
		providerMap.put(IndexConstants.ACCESSION_ID, "ID");
	}

	public void runLocal() throws Exception {
		doVocabAccessionID();
	}

	/**
	 * Gather the vocabulary accession ID's. Please note that AD has no
	 * accession ID's
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabAccessionID()
			throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up vocab accession id's, please note that AD has no acc id's
		// each additional union gets all the terms alt id's.
		// all the other vocabs could be add to index all ALT ids
		String VOC_ACCID_KEY = StrainUtils.withStrains
				+ " select _Term_key, accId, vocabName, _LogicaldB_key "
				+ "from VOC_Term_View where isObsolete != 1 "
				+ "and _Vocab_key in (125, 4, 5, 8, 46, 90, 91) "
				
				+ "union "
				
				// secondary DO terms from any provider
				
				+ "select vtv._Term_key, a.accId, 'Disease Ontology' as vocabName, a._LogicalDB_key "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 125 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0"

				+ "union "
				
				// add in numeric-only version of OMIM IDs for DO terms
				
				+ "select vtv._Term_key, a.numericPart::text, 'Disease Ontology' as vocabName, a._LogicalDB_key "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 125 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0 and a._LogicalDB_key = 15 "

				+ "union "
				
				+ "select vtv._Term_key, a.accId, 'Mammalian Phenotype' as vocabName, a._LogicalDB_key "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 5 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0"
				
				+ "union "
				
				+ "select vtv._Term_key, a.accId, 'EMAPA' as vocabName, a._LogicalDB_key "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 90 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				+ "and a.preferred = 0"
				
				+ "union "
				
				+ "select vtv._Term_key, a.accId, 'GO' as vocabName, a._LogicalDB_key "
				+ "from VOC_Term_View vtv, ACC_Accession a "
				+ "where isObsolete != 1 "
				+ "and vtv._Vocab_key = 4 and vtv._Term_key = a._Object_key and a._MGIType_key = 13 "
				
				+ "union "
				+ "select t._Strain_key, a.accID, 'Strain', a._LogicalDB_key "
				+ "from " + StrainUtils.strainTempTable + " t, acc_accession a "
				+ "where t._Strain_key = a._Object_key "
				+ " and a._MGIType_key = 10";

		// Gather the data

		ResultSet rs_acc_id = executor.executeMGD(VOC_ACCID_KEY);
		

		log.info("Time taken gather result set: " + executor.getTiming());

		// Parse it

		while (rs_acc_id.next()) {
			builder.setData(rs_acc_id.getString("accId"));
			builder.setRaw_data(rs_acc_id.getString("accId"));
			builder.setDb_key(StrainUtils.getDocumentKey(rs_acc_id.getString("_Term_key"), rs_acc_id.getString("vocabName")));
			builder.setVocabulary(rs_acc_id.getString("vocabName"));
			builder.setDataType(IndexConstants.ACCESSION_ID);
			builder.setDisplay_type(providerMap.get(IndexConstants.ACCESSION_ID));

			// for OMIM IDs that are associated with DO (Disease Ontology) terms, we need to ensure
			// that we show a special provider suffix.
			
			if (rs_acc_id.getString("vocabName").equals("Disease Ontology")) {
				if (rs_acc_id.getInt("_LogicalDB_key") == 15) {
					if (rs_acc_id.getString("accId").matches("^[0-9]+$")) {
						builder.setProvider("(OMIM)");
					}
				}
			}
			
			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		rs_acc_id.close();
		log.info("Done collecting Vocab Accession IDs!");

	}
}

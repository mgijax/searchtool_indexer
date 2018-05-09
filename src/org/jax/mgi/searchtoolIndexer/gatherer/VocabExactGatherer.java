package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.VocabExactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.StrainUtils;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * The vocab exact gatherer is responsible for gathering large token relevant
 * search information for our vocabulary searches.
 * 
 * This information is then used to populate the vocabExact index.
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
 *       Gather the data for the specific sub type, parse it while creating
 *       Lucene documents and adding them to the stack.
 * 
 *       After it completes parsing, it cleans up its result sets, and exits.
 * 
 *       After all of these methods complete, we set gathering complete to true
 *       in the shared document stack and exit.
 */

public class VocabExactGatherer extends DatabaseGatherer {

	// Class Variables

	private VocabExactLuceneDocBuilder	builder		= new VocabExactLuceneDocBuilder();

	private HashMap<String, String>		providerMap	= new HashMap<String, String>();

	public VocabExactGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.VOCAB_TERM, "Term");
		providerMap.put(IndexConstants.VOCAB_SYNONYM, "Synonym");
		providerMap.put(IndexConstants.VOCAB_NOTE, "Definition");
	}

	public void runLocal() throws Exception {
		doVocabTerm();
		doVocabSynonym();
		doVocabNote();
	}

	/**
	 * Gather the non AD Vocab Terms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerm() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up vocab term, that are not obsolete for all vocabs but ad.
		// Currently this list includes: GO, MP, Disease Ontology (DO), InterPro, and PIRSF
		// Lower part of the union brings in Strain synonyms, as we want to treat strains as a vocab for now.

		String VOC_TERM_KEY = StrainUtils.withStrains
				+ " select _Term_key, term, vocabName"
				+ " from VOC_Term_View"
				+ " where isObsolete != 1 and _Vocab_key in (125, 4, 5, 8, 46, 90)"
				+ " union "
				+ "select t._Strain_key, s.strain, 'Strain' "
				+ "from " + StrainUtils.strainTempTable + " t, prb_strain s "
				+ "where t._Strain_key = s._Strain_key";

		// Gather the data

		ResultSet rs_non_ad_term = executor.executeMGD(VOC_TERM_KEY);
		
		log.info("Time taken gather result set: " + executor.getTiming());

		// Parse it

		while (rs_non_ad_term.next()) {

			builder.setVocabulary(rs_non_ad_term.getString("vocabName"));
			builder.setData(rs_non_ad_term.getString("term"));
			builder.setRaw_data(rs_non_ad_term.getString("term"));
			builder.setDb_key(StrainUtils.getDocumentKey(rs_non_ad_term.getString("_Term_key"), rs_non_ad_term.getString("vocabName")));
			builder.setDataType(IndexConstants.VOCAB_TERM);
			builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_TERM));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		rs_non_ad_term.close();
		log.info("Done collecting Vocab Non AD Terms!");

	}

	/**
	 * Gather the non ad vocab synonyms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonym() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up vocab synonyms for all vocabularies with the exception of
		// AD.

		// Currently this list includes: GO, MP, Disease Ontology (DO), Interpro and PIRSF.
		// Lower part of the union brings in Strain synonyms, as we want to treat strains as a vocab for now.

		String VOC_SYN_KEY = StrainUtils.withStrains
				+ "select tv._Term_key, s.synonym, tv.vocabName"
				+ " from VOC_Term_View tv, MGI_Synonym s"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90)"
				+ " and s._MGIType_key = 13 "
				+ "union "
				+ "select s._Object_key, s.synonym, 'Strain' "
				+ "from mgi_synonym s "
				+ "inner join mgi_synonymtype t on (s._SynonymType_key = t._SynonymType_key and t._MGIType_key = 10) "
				+ "inner join " + StrainUtils.strainTempTable + " ps on (s._Object_key = ps._Strain_key)";

		// Gather the Data

		ResultSet rs_non_ad_syn = executor.executeMGD(VOC_SYN_KEY);

		log.info("Time taken gather result set: " + executor.getTiming());

		// Parse it

		while (rs_non_ad_syn.next()) {
			builder.setData(rs_non_ad_syn.getString("synonym"));
			builder.setRaw_data(rs_non_ad_syn.getString("synonym"));
			builder.setDb_key(StrainUtils.getDocumentKey(rs_non_ad_syn.getString("_Term_key"), rs_non_ad_syn.getString("vocabName")));
			builder.setVocabulary(rs_non_ad_syn.getString("vocabName"));
			builder.setDataType(IndexConstants.VOCAB_SYNONYM);
			builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_SYNONYM));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		rs_non_ad_syn.close();
		log.info("Done collecting Vocab Non AD Synonyms!");

	}

	/**
	 * Gather the non ad vocab notes/definitions. Please note that this is a
	 * compound field, and thus its parser has special handling.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNote() throws SQLException, InterruptedException {

		// SQL for this subsection

		// Gather up the vocabulary notes for all vocabs except for AD.
		// Currently this list includes: GO, MP, Disease Ontology (DO), PIRSH and Interpro.
		// No notes for strains.

		String VOC_NOTE_KEY = "select tv._Term_key, tv.note, tv.vocabName "
				+ " from VOC_Term_View tv "
				+ " where tv.note is not null"
				+ " and tv.isObsolete != 1"
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90)"
				+ " order by tv._Term_key";

		// Gather the data.

		ResultSet rs_non_ad_note = executor.executeMGD(VOC_NOTE_KEY);

		log.info("Time taken gather result set: " + executor.getTiming());

		// Parse it

		int place = -1;

		while (rs_non_ad_note.next()) {
			if (place != rs_non_ad_note.getInt("_Term_key")) {
				if (place != -1) {
					builder.setRaw_data(builder.getData());

					// Place the document on the stack.

					documentStore.push(builder.getDocument());
					builder.clear();
				}

				builder.setDb_key(rs_non_ad_note.getString("_Term_key"));
				builder.setVocabulary(rs_non_ad_note.getString("vocabName"));
				builder.setDataType(IndexConstants.VOCAB_NOTE);
				builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_NOTE));
				place = rs_non_ad_note.getInt("_Term_key");
			}
			builder.appendData(rs_non_ad_note.getString("note"));
		}

		// Clean up

		rs_non_ad_note.close();
		log.info("Done collecting Vocab Non AD Notes/Definitions");

	}

}

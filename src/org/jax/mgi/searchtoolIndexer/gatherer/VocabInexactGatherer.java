package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.VocabInexactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.StrainUtils;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * The vocab inexact gatherer is responsible for gathering the small token
 * relevant information from our vocabulary data sources.
 * 
 * This information is then used to populate the vocabInexact index.
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

public class VocabInexactGatherer extends DatabaseGatherer {

	// Class Variables

	private VocabInexactLuceneDocBuilder	builder		= new VocabInexactLuceneDocBuilder();

	HashMap<String, String>					providerMap	= new HashMap<String, String>();

	/**
	 * Get a new copy of the Vocab Inexact gatherer, and set up its hashmap.
	 * 
	 * @param config
	 */

	public VocabInexactGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.VOCAB_TERM, "Term");
		providerMap.put(IndexConstants.VOCAB_SYNONYM, "Synonym");
		providerMap.put(IndexConstants.VOCAB_NOTE, "Definition");
	}

	/**
	 * Encapsulates the algorithm used to gather up all the information needed
	 * for the Vocab Inexact index.
	 */

	public void runLocal() throws Exception {
		doVocabTerm();
		doVocabSynonym();
		doVocabNote();
	}

	/**
	 * Gather the Vocab Non AD Term data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void doVocabTerm() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up vocab term, that are not obsolete for all vocabs but ad.
		// Currently this list includes: GO, MP, Disease Ontology (DO), InterPro, and PIRSF.
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

		ResultSet rs = executor.executeMGD(VOC_TERM_KEY);
		
		log.info("Time taken gather non ad terms result set: " + executor.getTiming());

		// Parse it

		while (rs.next()) {

			builder.setData(rs.getString("term"));
			builder.setRaw_data(rs.getString("term"));
			builder.setDb_key(StrainUtils.getDocumentKey(rs.getString("_Term_key"), rs.getString("vocabName")));
			builder.setVocabulary(rs.getString("vocabName"));
			builder.setDataType(IndexConstants.VOCAB_TERM);
			builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_TERM));

			// Place the document on the stack

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		rs.close();
		log.info("Done gathering Vocab Non AD Terms!");

	}

	/**
	 * Gather the Vocab non AD Synonym data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void doVocabSynonym() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up vocab synonyms for all vocabularies with the exception of
		// AD.

		// Currently this list includes: GO, MP, Disease Ontology (DO), Interpro and PIRSF
		// Lower part of the union brings in Strain synonyms, as we want to treat strains as a vocab for now.

		String VOC_SYN_KEY = StrainUtils.withStrains 
				+ " select tv._Term_key, s.synonym, tv.vocabName"
				+ " from VOC_Term_View tv, MGI_Synonym s"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1 "
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90)"
				+ " and s._MGIType_key = 13 "
				+ "union "
				+ "select s._Object_key, s.synonym, 'Strain' "
				+ "from mgi_synonym s "
				+ "inner join mgi_synonymtype t on (s._SynonymType_key = t._SynonymType_key and t._MGIType_key = 10) "
				+ "inner join " + StrainUtils.strainTempTable + " ps on (s._Object_key = ps._Strain_key)";

		// gather the data

		ResultSet rs_syn = executor.executeMGD(VOC_SYN_KEY);
		
		log.info("Time taken gather non ad synonyms result set: " + executor.getTiming());

		// parse it

		while (rs_syn.next()) {

			builder.setData(rs_syn.getString("synonym"));
			builder.setRaw_data(rs_syn.getString("synonym"));
			builder.setDb_key(StrainUtils.getDocumentKey(rs_syn.getString("_Term_key"), rs_syn.getString("vocabName")));
			builder.setVocabulary(rs_syn.getString("vocabName"));
			builder.setDataType(IndexConstants.VOCAB_SYNONYM);
			builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_SYNONYM));

			// Place the document on the stack

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		rs_syn.close();
		log.info("Done gathering Vocab Non AD Synonyms!");
	}

	/**
	 * Gather the non AD note/Definition information. Please note that this is a
	 * compound data row as such its parsing code is a bit unique.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNote() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the vocabulary notes for all vocabs except for AD.
		// Currently this list includes: GO, MP, Disease Ontology (DO), PIRSH and Interpro.
		// No notes for strains.

		String VOC_NOTE_KEY = "select tv._Term_key, tv.note, tv.vocabName "
				+ " from VOC_Term_View tv "
				+ " where tv.note is not null"
				+ " and tv.isObsolete != 1 "
				+ " and tv._Vocab_key in (125, 4, 5, 8, 46, 90)"
				+ " order by tv._Term_key";

		// Gather the data

		// Since notes are compound rows in the database, we have to
		// construct the searchable field.

		ResultSet rs_note = executor.executeMGD(VOC_NOTE_KEY);
		
		log.info("Time taken gather non ad notes/definitions result set: " + executor.getTiming());

		// Parse it

		int place = -1;

		while (rs_note.next()) {
			if (place != rs_note.getInt("_Term_key")) {
				if (place != -1) {
					builder.setRaw_data(builder.getData());

					// Place the document on the stack

					documentStore.push(builder.getDocument());
					builder.clear();
				}
				builder.setDb_key(rs_note.getString("_Term_key"));
				builder.setVocabulary(rs_note.getString("vocabName"));
				builder.setDataType(IndexConstants.VOCAB_NOTE);
				builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_NOTE));
				place = rs_note.getInt("_Term_key");
			}
			builder.appendData(rs_note.getString("note"));
		}

		documentStore.push(builder.getDocument());

		// Clean up

		rs_note.close();
		log.info("Done gathering Vocab Non AD Notes!");

	}

}

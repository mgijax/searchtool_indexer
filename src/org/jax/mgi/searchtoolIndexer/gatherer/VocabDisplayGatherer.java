package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.VocabDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * The vocab display gatherer is responsible for gathering all the possible
 * display information for our various vocabulary data sources. Like all
 * vocabulary related gatherers it is split into non AD and AD data sources.
 * 
 * This information is then used to populate the vocabDisplay index.
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

public class VocabDisplayGatherer extends DatabaseGatherer {

	// Class Variables
	// Instantiate the single VocabDisplay Lucene doc builder.

	private VocabDisplayLuceneDocBuilder	builder		=
																new VocabDisplayLuceneDocBuilder();

	HashMap<String, String>					providerMap	= new HashMap<String, String>();

	// SQL Section

	public VocabDisplayGatherer(IndexCfg config) {
		super(config);

		providerMap.put(IndexConstants.GO_TYPE_NAME, "Gene Ontology");
		providerMap.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
		providerMap.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
		providerMap.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
		providerMap.put(IndexConstants.OMIM_TYPE_NAME, "Disease");
		providerMap.put(IndexConstants.GO_TYPE_NAME, "Function");
		providerMap.put(IndexConstants.AD_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPA_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPS_TYPE_NAME, "Expression");
	}

	public void runLocal() throws Exception {
		doNonADVocab();
		//doADVocab();
	}

	/**
	 * Gather up all of the display information for non AD vocab terms. This is
	 * by far the most complex thing we do in indexing, as such the code is
	 * documented inline much more carefully.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doNonADVocab() throws SQLException, InterruptedException {

		// Gather EMAPA stage ranges
		// Create a Hash of EMAPA stage names to term
		HashMap<String, String> termToEmapaStageMap = new HashMap<String, String>();
		String VOC_TERM_EMAPA_STAGES = "select _Term_key, startStage, endStage from VOC_Term_EMAPA";
		ResultSet emapa_stage_rs = executor.executeMGD(VOC_TERM_EMAPA_STAGES);
		while (emapa_stage_rs.next()) {
			termToEmapaStageMap.put(emapa_stage_rs.getString("_Term_key"), "TS" + emapa_stage_rs.getString("startStage") + "-" + emapa_stage_rs.getString("endStage"));
		}
		emapa_stage_rs.close();

		// Gather EMAPS stage ranges
		// Create a Hash of EMAPS stage names to term
		HashMap<String, String> termToEmapsStageMap = new HashMap<String, String>();
		String VOC_TERM_EMAPS_STAGE = "select _Term_key, stage from VOC_Term_EMAPS";
		ResultSet emaps_stage_rs = executor.executeMGD(VOC_TERM_EMAPS_STAGE);
		while (emaps_stage_rs.next()) {
			termToEmapsStageMap.put(emaps_stage_rs.getString("_Term_key"), "TS" + emaps_stage_rs.getString("stage"));
		}
		emaps_stage_rs.close();

		// Gather the marker key for a given term, in term key order.
		// Create a Hash of Terms to marker keys
		HashMap<String, ArrayList<String>> termToMarkerMap = new HashMap<String, ArrayList<String>>();
		String VOC_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key from VOC_Marker_Cache where annotType != 'AD'";
		ResultSet marker_display_rs = executor.executeMGD(VOC_MARKER_DISPLAY_KEY);
		while (marker_display_rs.next()) {
			ArrayList<String> markers = termToMarkerMap.get(marker_display_rs.getString("_Term_key"));
			if (markers == null) {
				markers = new ArrayList<String>();
				termToMarkerMap.put(marker_display_rs.getString("_Term_key"), markers);
			}
			markers.add(marker_display_rs.getString("_Marker_key"));
		}
		marker_display_rs.close();

		// Gather the marker count for markers directly annotated to a given
		// term, in term key order.
		// Create a Hash of Terms to marker counts
		HashMap<String, String> termToMarkerCountMap = new HashMap<String, String>();
		String VOC_NON_AD_MARKER_COUNT = "select _Term_key, count(_Marker_key)"
				+ " as marker_count from VOC_Marker_Cache"
				+ " where annotType !='AD'"
				+ " group by _Term_key";
		ResultSet vocab_marker_count_rs = executor.executeMGD(VOC_NON_AD_MARKER_COUNT);
		while (vocab_marker_count_rs.next()) {
			termToMarkerCountMap.put(vocab_marker_count_rs.getString("_Term_key"), vocab_marker_count_rs.getString("marker_count"));
		}
		vocab_marker_count_rs.close();

		// Grab the dag for a given vocab term, in term key order.
		// Create a Hash of Ancestor Object Keys to Descendent Object keys
		HashMap<String, ArrayList<String>> ancestorToDescendentMap = new HashMap<String, ArrayList<String>>();
		String VOC_DAG_KEY = "select _AncestorObject_key,"
				+ " _DescendentObject_key" + " from DAG_Closure"
				+ " where _MGIType_key = 13";
		ResultSet child_rs = executor.executeMGD(VOC_DAG_KEY);
		while (child_rs.next()) {
			ArrayList<String> descendents = ancestorToDescendentMap.get(child_rs.getString("_AncestorObject_key"));
			if (descendents == null) {
				descendents = new ArrayList<String>();
				ancestorToDescendentMap.put(child_rs.getString("_AncestorObject_key"), descendents);
			}
			descendents.add(child_rs.getString("_DescendentObject_key"));
		}
		child_rs.close();

		// Gather the number of annotations annotated directly to a given term
		// in term key order.
		// Create a Hash of Hashs to get all this data
		HashMap<String, HashMap<String, String>> termToAnnotationsCountMap = new HashMap<String, HashMap<String, String>>();
		String VOC_NON_AD_ANNOT_COUNT = "select _Term_key, _MGIType_key, "
				+ "objectCount, annotCount" + " from VOC_Annot_Count_Cache"
				+ " where annotType != 'AD'";
		ResultSet vocab_annot_rs = executor.executeMGD(VOC_NON_AD_ANNOT_COUNT);
		while (vocab_annot_rs.next()) {
			HashMap<String, String> annotations = termToAnnotationsCountMap.get(vocab_annot_rs.getString("_Term_key"));
			if (annotations == null) {
				annotations = new HashMap<String, String>();
				termToAnnotationsCountMap.put(vocab_annot_rs.getString("_Term_key"), annotations);
			}
			annotations.put("_MGIType_key", vocab_annot_rs.getString("_MGIType_key"));
			annotations.put("objectCount", vocab_annot_rs.getString("objectCount"));
			annotations.put("annotCount", vocab_annot_rs.getString("annotCount"));
		}
		vocab_annot_rs.close();

		// Since this is a compound object, the order by clauses are important.

		// Gather up the term, term key, accession id, and vocabulary name
		// in term key order.

		String GEN_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName" + " FROM VOC_Term_View tv"
				+ " where tv.isObsolete != 1 and tv._Vocab_key in "
				+ "(44, 4, 5, 8, 46, 90, 91)" + " order by _Term_key";

		ResultSet rs_vocabTerm = executor.executeMGD(GEN_VOC_KEY);

		log.info("Time taken gather Non AD Display result sets: " + executor.getTiming());
		int place = -1;

		/*
		 * These documents are compound in nature, for each document we create,
		 * we are running several queries into the database. This is most
		 * definitely the single most complicated document that we create for
		 * the entire indexing process.
		 */

		while (rs_vocabTerm.next()) {

			// Populate the document with information pertaining
			// specifically to the vocab term we are now on.

			builder.setDb_key(rs_vocabTerm.getString("_Term_key"));
			builder.setVocabulary(rs_vocabTerm.getString("vocabName"));
			builder.setTypeDisplay(providerMap.get(rs_vocabTerm.getString("vocabName")));
			builder.setAcc_id(rs_vocabTerm.getString("accID"));

			// This has to come after the setVocabulary
			if (builder.getVocabulary().equals(IndexConstants.EMAPA_TYPE_NAME)) {
				builder.setData(rs_vocabTerm.getString("term") + " " + termToEmapaStageMap.get(rs_vocabTerm.getString("_Term_key")));
			} else if (builder.getVocabulary().equals(IndexConstants.EMAPS_TYPE_NAME)) {
				builder.setData(termToEmapsStageMap.get(rs_vocabTerm.getString("_Term_key")) + ": " + rs_vocabTerm.getString("term"));
			} else {
				builder.setData(rs_vocabTerm.getString("term"));
			}

			// Find all of the genes that are directly annotated to this
			// vocabulary term, and append them into this document

			ArrayList<String> markers = termToMarkerMap.get(rs_vocabTerm.getString("_Term_key"));
			if (markers != null) {
				for (String marker : markers) {
					builder.appendGene_ids(marker);
				}
			}

			// Count the number of markers directly annotated to this
			// object.

			if (termToMarkerCountMap.get(rs_vocabTerm.getInt("_Term_key")) != null) {
				builder.setMarker_count(termToMarkerCountMap.get(rs_vocabTerm.getInt("_Term_key")));
			}

			// Add in all the other vocabulary terms that are children on
			// this term in term_key order.

			ArrayList<String> descendents = ancestorToDescendentMap.get(rs_vocabTerm.getString("_Term_key"));
			if (descendents != null) {
				for (String descendent : descendents) {
					builder.appendChild_ids(descendent);
				}
			}

			// Set the annotation counts, and in the case of non human
			// omim, the secondary object counts.

			HashMap<String, String> annotations = termToAnnotationsCountMap.get(rs_vocabTerm.getString("_Term_key"));
			if (annotations != null) {
				if (!builder.getVocabulary().equals(IndexConstants.OMIM_TYPE_NAME) || (builder.getVocabulary().equals(IndexConstants.OMIM_TYPE_NAME) && "12".equals(annotations.get("_MGIType_key")))) {
					builder.setAnnotation_object_type(annotations.get("_MGIType_key"));
					builder.setAnnotation_objects(annotations.get("objectCount"));
					builder.setAnnotation_count(annotations.get("annotCount"));
				} else {
					builder.setSecondary_object_count(annotations.get("annotCount"));
				}
			}

			documentStore.push(builder.getDocument());
			builder.clear();

		}

		// Clean up

		log.info("Done Non AD Display Information!");

		rs_vocabTerm.close();
	}

	/**
	 * Gather up all of the AD Term's display information, please note that
	 * while similar to non ad terms, AD is special in that it doesn't possess
	 * Accession ID's.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doADVocab() throws SQLException, InterruptedException {

		// SQL For this Subsection

		// Gather the count of markers directly annotated to a given term
		// in term key order.

		String VOC_AD_MARKER_COUNT = "select _Term_key, count(_Marker_key)"
				+ " as marker_count" + " from VOC_Marker_Cache"
				+ " where annotType ='AD'"
				+ " group by _Term_key order by _Term_key";

		// Gather the printname, the vocab name, the number of objects annotated
		// to this term, and the number of annotations, in term key order.

		String GEN_AD_KEY =
				"SELECT distinct s._Structure_key, "
						+ "  'TS' || _Stage_key || ': ' || printName as PrintName2, "
						+ "  'AD' as VocabName, "
						+ "  vac.objectCount, "
						+ "  vac.annotCount, "
						+ "  vac._MGIType_key "
						+ "FROM GXD_Structure s "
						+ "inner join GXD_StructureName sn on ("
						+ "  s._Structure_key = sn._Structure_key)"
						+ "left outer join VOC_Annot_Count_Cache vac on ("
						+ "  s._Structure_key = vac._Term_key "
						+ "  and vac.annotType = 'AD') "
						+ "where s._Parent_key is not null "
						+ "order by s._Structure_key";

		// Get the dag for a given ad term, in term key order.

		String AD_VOC_DAG_KEY = "select _Structure_key, _Descendent_key"
				+ " from GXD_StructureClosure"
				+ " order by _Structure_key, _Descendent_key";

		// Gather the marker keys for a given term in term key order.

		String VOC_AD_MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key  from VOC_Marker_Cache"
				+ " where annotType = 'AD'" + " order by _Term_key";

		ResultSet rs_ad = executor.executeMGD(GEN_AD_KEY);

		ResultSet ad_marker_display_rs = executor.executeMGD(VOC_AD_MARKER_DISPLAY_KEY);
		ad_marker_display_rs.next();

		ResultSet vocab_marker_count_rs_ad = executor.executeMGD(VOC_AD_MARKER_COUNT);
		vocab_marker_count_rs_ad.next();

		ResultSet ad_child_rs = executor.executeMGD(AD_VOC_DAG_KEY);
		ad_child_rs.next();

		log.info("Time taken gather AD Display result sets: "
				+ executor.getTiming());

		// Since these are compound documents, we need to keep track of the
		// document we are on.

		int place = -1;

		while (rs_ad.next()) {

			// Have we found a new document?

			if (place != rs_ad.getInt("_Structure_key")) {

				// If so, and its not the first document, add the current
				// document to the stack.

				if (place != -1) {
					documentStore.push(builder.getDocument());
					builder.clear();
				}

				// Populate the basic document information.

				builder.setDb_key(rs_ad.getString("_Structure_key"));
				builder.setData(rs_ad.getString("PrintName2"));
				builder.setVocabulary(rs_ad.getString("VocabName"));
				builder.setTypeDisplay(providerMap.get(rs_ad.getString("vocabName")));
				builder.setAnnotation_object_type(rs_ad.getString("_MGIType_key"));
				builder.setAnnotation_objects(rs_ad.getString("objectCount"));
				builder.setAnnotation_count(rs_ad.getString("annotCount"));

				// Set the document place, when this changes we know we are on a
				// new document.

				place = rs_ad.getInt("_Structure_key");

				// Grab the markers directly associated to these terms.

				while (!ad_marker_display_rs.isAfterLast()
						&& ad_marker_display_rs.getInt("_Term_key") <= place) {
					if (ad_marker_display_rs.getInt("_Term_key") == place) {
						builder.appendGene_ids(ad_marker_display_rs.getString("_Marker_key"));
					}
					ad_marker_display_rs.next();
				}

				// Grab the marker counts

				while (!vocab_marker_count_rs_ad.isAfterLast() && vocab_marker_count_rs_ad.getInt("_Term_key") < place) {
					vocab_marker_count_rs_ad.next();
				}
				if (!vocab_marker_count_rs_ad.isAfterLast() && vocab_marker_count_rs_ad.getInt("_Term_key") == place) {
					builder.setMarker_count(vocab_marker_count_rs_ad.getString("marker_count"));
				}

				// Grab the terms that are descendants of this term, by term
				// key order.

				while (!ad_child_rs.isAfterLast() && ad_child_rs.getInt("_Structure_key") <= place) {
					if (ad_child_rs.getInt("_Structure_key") == place) {
						builder.appendChild_ids(ad_child_rs.getString("_Descendent_key"));
					}
					ad_child_rs.next();
				}
			}
		}

		// Push the last document onto the stack, the one the loop kicked
		// out on.

		documentStore.push(builder.getDocument());

		// Clean up

		log.info("Done AD Display Inforamtion!");

		rs_ad.close();
		ad_marker_display_rs.close();
		vocab_marker_count_rs_ad.close();
		ad_child_rs.close();
	}

}

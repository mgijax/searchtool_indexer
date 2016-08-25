package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureVocabDagLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gatherings up the relationiships between a
 * given vocabulary term, and the markers that are directly annotated to it. We
 * also gather up the parent/child relationships for a given term.
 *
 * This information is then used to populate the markerVocabDag index.
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

public class GenomeFeatureVocabDagGatherer extends DatabaseGatherer {

	// Class Variables
	// Instantiate the single VocabDisplay Lucene doc builder.

	private GenomeFeatureVocabDagLuceneDocBuilder	builder	= new GenomeFeatureVocabDagLuceneDocBuilder();

	public GenomeFeatureVocabDagGatherer(IndexCfg config) {
		super(config);
	}

	public void runLocal() throws Exception {
		doNonADVocabs();
	}

	/**
	 * Gather up all of the display information for non AD vocab terms. We do
	 * this individually for each vocabulary type, so if requirements change for
	 * any specific type, we can change them independently.
	 *
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doNonADVocabs() throws SQLException, InterruptedException {

		log.info("Collecting Non AD Dag Information");

		// Since this is a compound object, the order by clauses are important.

		log.info("Collecting GO Records!");

		VocabSpec vsGO = new VocabSpec();

		// GO

		// Gather term keys, terms, accession ID and vocabulary names for GO
		// where the term is not obsolete.

		String GO_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 4"
				+ " and vacc.annotType = 'GO/Marker' and vacc._Term_key ="
				+ " tv._Term_key" + " order by _Term_key";

		vsGO.setVoc_key(GO_VOC_KEY);

		// Gather the marker keys, for a given term key for go.

		String GO_MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key " + "from VOC_Marker_Cache"
				+ " where annotType = 'GO/Marker'" + " order by _Term_key";

		// Gather the dag for a given go term.

		vsGO.setDisplay_key(GO_MARKER_DISPLAY_KEY);

		String GO_DAG_KEY = "select dc._AncestorObject_key,"
				+ " dc._DescendentObject_key"
				+ " from DAG_Closure dc, VOC_Annot_Count_Cache vacc, "
				+ " VOC_Term vt, VOC_AnnotType vat"
				+ " where dc._MGIType_key = 13"
				+ " and dc._DescendentObject_key = vacc._Term_key"
				+ " and vt._Term_key =dc._DescendentObject_key "
				+ " and vt._Vocab_key = vat._Vocab_key and vat.name ="
				+ " vacc.annotType " + "and vacc.annotType = 'GO/Marker'"
				+ " order by dc._AncestorObject_key, dc._DescendentObject_key";

		vsGO.setDag_key(GO_DAG_KEY);
		vsGO.setObject_type("MARKER");

		doSingleNonADVocab(vsGO);


		// For now EMAPA results are not returned oblod 1/23/2014

		log.info("Collecting EMAPS Records!");

		// Gather term key, term, accession id, and vocabulary name for EMAPS

		VocabSpec vsEMAPS = new VocabSpec();

		String EMAPS_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 91 and vacc.annotType = 'EMAPS' and"
				+ " vacc._Term_key = tv._Term_key" + " order by _Term_key";

		vsEMAPS.setVoc_key(EMAPS_VOC_KEY);

		// Gather the marker keys for given emaps terms.

		// The expression flag in the GXD_Expression table includes "Ambiguous Results"
		// This query takes the GXD Expression and removes the ambiguous results via the gisr._Strength_key in (2, 4, 5, 6, 7, 8)
		// Select * from GXD_Strength to get a list of Strength's
		// This query also uses the new EMAPS_Mapping table to get only id's of AD terms that are mapped to EMAPS terms
		// This query also has to look at Insitu results and at GelLane results

		String EMAPS_MARKER_DISPLAY_KEY = "select"
			+ " distinct ge._Marker_key as _Marker_key, vts._Term_key as _Term_key "
			+ " from  MRK_Marker mm, GXD_Expression ge, GXD_Assay ga, "
			+ "  GXD_Specimen gs, GXD_InSituResult gisr, "
			+ "  GXD_ISResultStructure girs, VOC_Term_EMAPS vts "
			+ " where ge._Marker_key = mm._Marker_key "
			+ "   and mm._Marker_Status_key != 2 "
			+ "   and ge.expressed = 1 "
			+ "   and ge.isForGXD = 1 "
			+ "   and ge._Marker_key = ga._Marker_key "
			+ "   and ga._Assay_key = gs._Assay_key "
			+ "   and gs._Specimen_key = gisr._Specimen_key "
			+ "   and gisr._Result_key = girs._Result_key "
			+ "   and gisr._Strength_key in (2, 4, 5, 6, 7, 8) "
			+ "   and girs._EMAPA_Term_key = ge._EMAPA_Term_key "
			+ "   and girs._Stage_key = ge._Stage_key "
			+ "   and ge._EMAPA_Term_key = vts._EMAPA_Term_key "
			+ "   and ge._Stage_key = vts._Stage_key "
			+ " union"
			+ " select distinct "
			+ "  ge._Marker_key as _Marker_key, vts._Term_key as _Term_key "
			+ " from MRK_Marker mm, GXD_Expression ge, GXD_Assay ga, GXD_GelLane ggl,"
			+ "  GXD_GelLaneStructure ggs, GXD_GelBand gb, VOC_Term_EMAPS vts "
			+ " where ge._Marker_key = mm._Marker_key "
			+ "  and mm._Marker_Status_key != 2 "
			+ "  and ge.expressed = 1 "
			+ "  and ge.isForGXD = 1 "
			+ "  and ge._Marker_key = ga._Marker_key "
			+ "  and ga._Assay_key = ggl._Assay_key "
			+ "  and ggl._GelLane_key = gb._GelLane_key "
			+ "  and gb._Strength_key in (2, 4, 5, 6, 7, 8) "
			+ "  and ggl._GelLane_key = ggs._GelLane_key "
			+ "  and ge._EMAPA_Term_key = ggs._EMAPA_Term_key "
			+ "  and ge._Stage_key = ggs._Stage_key "
			+ "  and ge._EMAPA_Term_key = vts._EMAPA_Term_key "
			+ "  and ge._Stage_key = vts._Stage_key "
			+ "  order by _Term_key ";


		vsEMAPS.setDisplay_key(EMAPS_MARKER_DISPLAY_KEY);

		// Gather the dag for a given emaps term.


		String EMAPS_DAG_KEY = "select dc._AncestorObject_key,"
				+ " dc._DescendentObject_key"
				+ " from DAG_Closure dc, VOC_Annot_Count_Cache vacc, VOC_Term vt"
				+ " where dc._MGIType_key = 13"
				+ " and vt._Vocab_key = 91"
				+ " and dc._DescendentObject_key = vacc._Term_key"
				+ " and vt._Term_key = dc._DescendentObject_key "
				+ " and vacc.annotType = 'EMAPS'"
				+ " order by dc._AncestorObject_key, dc._DescendentObject_key";

		vsEMAPS.setDag_key(EMAPS_DAG_KEY);
		vsEMAPS.setObject_type("MARKER");

		doSingleNonADVocab(vsEMAPS);


		log.info("Collecting MP Records!");

		// Gather term key, term, accession id, and vocabulary name for MP

		VocabSpec vsMP = new VocabSpec();

		String MP_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 5"
				+ " and vacc.annotType = 'Mammalian Phenotype/Genotype'"
				+ " and vacc._Term_key = tv._Term_key" + " order by _Term_key";

		vsMP.setVoc_key(MP_VOC_KEY);

		// Gather the marker keys for given mp terms.

		String MP_MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key " + "from VOC_Marker_Cache"
				+ " where annotType = 'Mammalian Phenotype/Genotype'"
				+ " order by _Term_key";

		vsMP.setDisplay_key(MP_MARKER_DISPLAY_KEY);

		// Gather the dag for a given mp term.

		String MP_DAG_KEY = "select dc._AncestorObject_key,"
				+ " dc._DescendentObject_key"
				+ " from DAG_Closure dc, VOC_Annot_Count_Cache vacc,"
				+ " VOC_Term vt, VOC_AnnotType vat"
				+ " where dc._MGIType_key = 13"
				+ " and dc._DescendentObject_key = vacc._Term_key"
				+ " and vt._Term_key =dc._DescendentObject_key "
				+ " and vt._Vocab_key = vat._Vocab_key and vat.name ="
				+ " vacc.annotType"
				+ " and vacc.annotType = 'Mammalian Phenotype/Genotype'"
				+ " order by dc._AncestorObject_key, dc._DescendentObject_key";

		vsMP.setDag_key(MP_DAG_KEY);
		vsMP.setObject_type("MARKER");

		doSingleNonADVocab(vsMP);

		log.info("Collecting MP Alleles Records!");

		// Gather term key, term, accession id, and vocabulary name for MP

		VocabSpec vsMPAllele = new VocabSpec();

		String MP_VOC_ALLELE_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Allele_Cache vac"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 5"
				+ " and vac.annotType = 'Mammalian Phenotype/Genotype'"
				+ " and vac._Term_key = tv._Term_key" + " order by _Term_key";

		vsMPAllele.setVoc_key(MP_VOC_ALLELE_KEY);

		// Gather the allele keys for given mp terms.

		String MP_ALLELE_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Allele_key as _Marker_key" + " from VOC_Allele_Cache"
				+ " where annotType = 'Mammalian Phenotype/Genotype'"
				+ " order by _Term_key";

		vsMPAllele.setDisplay_key(MP_ALLELE_DISPLAY_KEY);

		// Gather the dag for a given mp term.

		String MP_ALLELE_DAG_KEY = "select dc._AncestorObject_key,"
				+ " dc._DescendentObject_key"
				+ " from DAG_Closure dc, VOC_Allele_Cache vac,"
				+ " VOC_Term vt, VOC_AnnotType vat"
				+ " where dc._MGIType_key = 13"
				+ " and dc._DescendentObject_key = vac._Term_key"
				+ " and vt._Term_key =dc._DescendentObject_key "
				+ " and vt._Vocab_key = vat._Vocab_key and vat.name ="
				+ " vac.annotType"
				+ " and vac.annotType = 'Mammalian Phenotype/Genotype'"
				+ " order by dc._AncestorObject_key, dc._DescendentObject_key";

		vsMPAllele.setDag_key(MP_ALLELE_DAG_KEY);
		vsMPAllele.setObject_type("ALLELE");

		doSingleNonADVocab(vsMPAllele);

		// Interpro

		log.info("Collecting Interpro Records!");

		// Gather term key, term, accession id, and vocabulary name for interpro

		VocabSpec vsInterpro = new VocabSpec();

		String INTERPRO_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 8"
				+ " and vacc.annotType = 'InterPro/Marker' and"
				+ " vacc._Term_key = tv._Term_key" + " order by _Term_key";

		vsInterpro.setVoc_key(INTERPRO_VOC_KEY);

		// Gather the marker keys for given interpro terms.

		String INTERPRO_MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key " + "from VOC_Marker_Cache"
				+ " where annotType = 'InterPro/Marker'"
				+ " order by _Term_key";

		vsInterpro.setDisplay_key(INTERPRO_MARKER_DISPLAY_KEY);
		vsInterpro.setObject_type("MARKER");

		doSingleNonADVocab(vsInterpro);

		
		// PROTIEN ISOFORM ONTOLOGY

		log.info("Collecting Protien Isoform Ontology Records!");

		// Gather term key, term, accession id, and vocabulary

		VocabSpec vsProtIso = new VocabSpec();

		String PROT_ISO_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 112"
				+ " order by _Term_key";

		vsProtIso.setVoc_key(PROT_ISO_VOC_KEY);

		// Gather the marker keys for given pirsf terms.

		String PROT_ISO__MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key " + "from VOC_Marker_Cache"
				+ " where annotType = 'Protein Isoform Ontology/Marker'" + " order by _Term_key";

		vsProtIso.setDisplay_key(PROT_ISO__MARKER_DISPLAY_KEY);
		vsProtIso.setObject_type("MARKER");

		doSingleNonADVocab(vsProtIso);


		// PIRSF

		log.info("Collecting PIRSF Records!");

		// Gather term key, term, accession id, and vocabulary name for pirsf

		VocabSpec vsPIRSF = new VocabSpec();

		String PIRSF_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 46"
				+ " and vacc.annotType = 'PIRSF/Marker' and vacc._Term_key ="
				+ " tv._Term_key" + " order by _Term_key";

		vsPIRSF.setVoc_key(PIRSF_VOC_KEY);

		// Gather the marker keys for given pirsf terms.

		String PIRSF_MARKER_DISPLAY_KEY = "select distinct _Term_key,"
				+ " _Marker_key " + "from VOC_Marker_Cache"
				+ " where annotType = 'PIRSF/Marker'" + " order by _Term_key";

		vsPIRSF.setDisplay_key(PIRSF_MARKER_DISPLAY_KEY);
		vsPIRSF.setObject_type("MARKER");

		doSingleNonADVocab(vsPIRSF);

		// OMIM

		log.info("Collecting OMIM Marker Records!");

		// Gather term key, term, accession id, and vocabulary name for omin
		// non human.

		VocabSpec vsOMIM = new VocabSpec();

		String OMIM_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 44"
				+ " and vacc.annotType = 'OMIM/Genotype' and vacc._Term_key ="
				+ " tv._Term_key" + " order by _Term_key";

		vsOMIM.setVoc_key(OMIM_VOC_KEY);

		// Gather the marker keys for given omim non human terms. (via
		// annotations of OMIM disease terms to mouse genotypes)
		//
		// Excludes: Transgenes involving Cre

		String OMIM_MARKER_DISPLAY_KEY = "select distinct vmc._Term_key,"
				+ " vmc._Marker_key"
				+ " from VOC_Marker_Cache vmc, mrk_label ml"
				+ " where annotType = 'OMIM/Genotype' and vmc._Marker_key = "
				+ " ml._Marker_key and ml.label not like 'tg%cre%' and"
				+ " ml.labelType = 'MS'" + " order by _Term_key";

		vsOMIM.setDisplay_key(OMIM_MARKER_DISPLAY_KEY);
		vsOMIM.setObject_type("MARKER");

		doSingleNonADVocab(vsOMIM);

		log.info("Collecting OMIM Alleles Records!");

		// Gather term key, term, accession id, and vocabulary name for omin
		// non human.

		VocabSpec vsOMIMAllele = new VocabSpec();

		String OMIM_VOC_ALLELE_KEY = "SELECT tv._Term_key, tv.term,  tv.accID,"
				+ " tv.vocabName"
				+ " FROM VOC_Term_View tv, VOC_Allele_Cache vac"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 44"
				+ " and vac.annotType = 'OMIM/Genotype' and vac._Term_key ="
				+ " tv._Term_key" + " order by _Term_key";

		vsOMIMAllele.setVoc_key(OMIM_VOC_ALLELE_KEY);

		// Gather the marker keys for given omim non human terms.

		String OMIM_ALLELE_DISPLAY_KEY = "select distinct vac._Term_key,"
				+ " vac._Allele_key as _Marker_key"
				+ " from VOC_Allele_Cache vac, all_label al"
				+ " where annotType = 'OMIM/Genotype' and vac._Allele_key = "
				+ " al._Allele_key and al.labelType = 'AS'"
				+ " order by _Term_key";

		vsOMIMAllele.setDisplay_key(OMIM_ALLELE_DISPLAY_KEY);
		vsOMIMAllele.setObject_type("ALLELE");

		doSingleNonADVocab(vsOMIMAllele);

		// OMIM Human Orthologs

		VocabSpec vsOrtho = new VocabSpec();

		// Gather term key, term, accession id, and vocabulary name for omim
		// human.

		String OMIM_HUMAN_VOC_KEY = "SELECT tv._Term_key, tv.term, tv.accID,"
				+ " '" + IndexConstants.OMIM_ORTH_TYPE_NAME + "' as vocabName"
				+ " FROM VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 44"
				+ " and vacc.annotType = 'OMIM/Human Marker'"
				+ " and vacc._Term_key =" + " tv._Term_key"
				+ " order by _Term_key";

		vsOrtho.setVoc_key(OMIM_HUMAN_VOC_KEY);

		// Gather the marker keys for given omim/human terms. (via the
		// VOC_Marker_Cache -- human marker OMIM IDs, not OMIM disease IDs)
		//
		// Excludes: Transgenes involving Cre

		// new query using HomoloGene relationships, avoids VOC_Marker_Cache:
		String OMIM_HUMAN_MARKER_DISPLAY_KEY =

			"select distinct m._Marker_key, a._Term_key from voc_annot a, voc_term vt, mrk_marker h, mrk_clustermember hcm, mrk_cluster mc, mrk_clustermember mcm, mrk_marker m " +
			"where vt._term_key = a._term_key and a._AnnotType_key = 1006 and a._object_key = h._marker_key and h._organism_key = 2 and h._marker_key = hcm._marker_key and " +
			"hcm._cluster_key = mc._cluster_key and mc._ClusterSource_key = 13764519 and mc._ClusterType_key = 9272150 and mc._cluster_key = mcm._cluster_key and " +
			"mcm._marker_key = m._marker_key and m._organism_key = 1 and m._Marker_Status_key in (1,3) order by a._Term_key";

//				" select distinct m._Marker_key, a._Term_key "
//						+ "from VOC_Annot a, MRK_Cluster mc, MRK_ClusterMember mcm, "
//						+ "  MRK_ClusterMember mcm2, VOC_Term vt, MRK_Marker m "
//						+ "where mcm._Marker_key = a._Object_key "
//						+ " and a._AnnotType_key = 1006 "
//						+ " and m.symbol not like 'tg%cre%' "
//						+ " and mc._ClusterSource_key = vt._Term_key "
//						+ " and vt.term = 'HomoloGene and HGNC' "
//						+ " and m._Marker_Status_key != 2 "
//						+ " and mcm._Cluster_key = mc._Cluster_key "
//						+ " and mc._Cluster_key = mcm2._Cluster_key "
//						+ " and mcm2._Marker_key = m._Marker_key "
//						+ " and m._Organism_key = 1 "
//						+ "order by a._Term_key";





		vsOrtho.setDisplay_key(OMIM_HUMAN_MARKER_DISPLAY_KEY);
		vsOrtho.setObject_type("MARKER");

		doSingleNonADVocab(vsOrtho);

		log.info("Done Collecting Non AD Dag Information Records!");

	}

	/**
	 * Gather up all of the display information for non AD vocab terms. This
	 * method assumes that it will be given three sql strings, one to define the
	 * term itself, one to define the set of markers to assign to terms for a
	 * given vocabulary, and one to define the children of the given term.
	 *
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doSingleNonADVocab(VocabSpec vs) throws SQLException, InterruptedException {
		ResultSet rs_vocabTerm = executor.executeMGD(vs.getVoc_key());

		ResultSet marker_display_rs = executor.executeMGD(vs.getDisplay_key());
		marker_display_rs.next();

		ResultSet child_rs = null;

		if (vs.getDag_key() != null) {
			child_rs = executor.executeMGD(vs.getDag_key());
			child_rs.next();
		}

		log.info("Time taken gather result set: " + executor.getTiming());
		int place = -1;

		/*
		 * These documents are compound in nature, for each document we create,
		 * we are running several queries into the database This is most easily
		 * the single most complicated document that we create for the entire
		 * indexing process.
		 */

		while (rs_vocabTerm.next()) {

			// Have we found a new document?

			if (place != rs_vocabTerm.getInt(1)) {
				if (place != -1) {

					// We have, try to put it on the stack, waiting if
					// the stack is busy.
					//while (documentStore.size() > 100000) {
					//	Thread.sleep(1);
					//}

					// Place the current document on the stack.

					documentStore.push(builder.getDocument());

					// Clear the document creation object, to ready it for the
					// next doc.

					builder.clear();
				}

				// Populate the document with information pertaining
				// specifically to the vocab term we are now on.

				builder.setDb_key(rs_vocabTerm.getString("_Term_key"));
				builder.setVocabulary(rs_vocabTerm.getString("vocabName"));
				builder.setAcc_id(rs_vocabTerm.getString("accID"));
				builder.setUnique_key(rs_vocabTerm.getString("_Term_key") + rs_vocabTerm.getString("vocabName"));
				builder.setObject_type(vs.getObject_type());

				// Set the place to be the current terms object key, when this
				// changes we know we are on a new document.

				place = (int) rs_vocabTerm.getInt("_Term_key");

				// Find all of the genes that are directly annotated to this
				// vocabulary term, and append them into this document
				while (!marker_display_rs.isAfterLast() && marker_display_rs.getInt("_Term_key") <= place) {
					if (marker_display_rs.getInt("_Term_key") == place) {
						builder.appendGene_ids(marker_display_rs.getString("_Marker_key"));
					}
					marker_display_rs.next();
				}

				// Add in all the other vocabulary terms that are children
				// on this term in term_key order.
				if (child_rs != null) {
					while (!child_rs.isAfterLast() && child_rs.getInt("_AncestorObject_key") <= place) {
						if (child_rs.getInt("_AncestorObject_key") == place) {
							builder.appendChild_ids(child_rs.getString("_DescendentObject_key"));
						}
						child_rs.next();

					}
				}
			}
		}

		// Add the Final Document, that the loop kicked out on.

		documentStore.push(builder.getDocument());
		builder.clear();

		// Clean up

		rs_vocabTerm.close();
		marker_display_rs.close();
		if (child_rs != null) {
			child_rs.close();
		}
	}

}

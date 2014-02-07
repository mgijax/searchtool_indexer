package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureInexactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible to gather up anything we might want to match a
 * marker on inexactly. Meaning where punctuation is non important, and prefix
 * searching will be allowed.
 * 
 * This information is then used to populate the markerInexact index.
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

public class GenomeFeatureInexactGatherer extends DatabaseGatherer {

	// Instantiate the single doc builder that this object will use.

	private GenomeFeatureInexactLuceneDocBuilder builder = new GenomeFeatureInexactLuceneDocBuilder();

	public static HashMap<String, String> providerMap = new HashMap<String, String>();

	public GenomeFeatureInexactGatherer(IndexCfg config) {
		super(config);

		/*
		 * Please notice that three of these hashmap values are from a slightly
		 * different source than the others. Interpro, pirsf and mp can have a
		 * longer type name in some tables. For this dataset we need to use the
		 * longer names.
		 */

		providerMap.put(IndexConstants.MARKER_SYMBOL, "Marker Symbol");
		providerMap.put(IndexConstants.MARKER_NAME, "Marker Name");
		providerMap.put(IndexConstants.MARKER_SYNOYNM, "Marker Synonym");
		providerMap.put(IndexConstants.ALLELE_SYMBOL, "Allele Symbol");
		providerMap.put(IndexConstants.ALLELE_NAME, "Allele Name");
		providerMap.put(IndexConstants.ALLELE_SYNONYM, "Allele Synonym");
		providerMap.put(IndexConstants.ORTHOLOG_SYMBOL, "Ortholog Symbol");
		providerMap.put(IndexConstants.ORTHOLOG_NAME, "Ortholog Name");
		providerMap.put(IndexConstants.ORTHOLOG_SYNONYM, "Ortholog Synonym");
		providerMap.put(IndexConstants.GO_TYPE_NAME, "Gene Ontology");
		providerMap.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
		providerMap.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
		providerMap.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
		providerMap.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
		providerMap.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
		providerMap.put(IndexConstants.GO_TYPE_NAME, "Function");
		providerMap.put(IndexConstants.AD_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPA_TYPE_NAME, "Expression");
		providerMap.put(IndexConstants.EMAPS_TYPE_NAME, "Expression");
	}

	public void runLocal() throws Exception {
		doMarkerLabels();
		doVocabTerms();
		doVocabSynonyms();
		doVocabNotes();
		//doVocabADTerm();
		//doVocabADSynonym();
		doAlleleNomen();
	}

	/**
	 * Gather the marker labels.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doMarkerLabels() throws SQLException, InterruptedException {

		// SQL for this Subsection

		log.info("Collecting Marker Labels");

		// Collect all marker lables where the marker is not withdrawn
		// and the organism is for mouse.
		//
		// Includes: data for current or iterim mouse markers
		// Excludes:
		// allele synonyms
		// allele names
		// all data for transgene markers

		String MARKER_LABEL_KEY = "select ml._Marker_key, ml.label, "
				+ "ml.labelType, ml._OrthologOrganism_key, "
				+ "ml._Label_Status_key, ml.labelTypeName, ml._Label_key"
				+ " from MRK_Label ml, MRK_Marker m"
				+ " where ml._Organism_key = 1 and ml._Marker_key = "
				+ "m._Marker_key and m._Marker_Status_key !=2 "
				+ " and labelType not in ('AS','AN') "
				+ " and m._Marker_Type_key != 12";

		// Gather the data

		ResultSet rs = executor.executeMGD(MARKER_LABEL_KEY);
		rs.next();

		log.info("Time taken gather marker label result set "
				+ executor.getTiming());

		// Parse it

		String displayType = "";

		while (!rs.isAfterLast()) {

			displayType = InitCap.initCap(rs.getString("labelTypeName"));

			builder.setData(rs.getString("label"));
			builder.setRaw_data(rs.getString("label"));
			builder.setDb_key(rs.getString("_Marker_key"));
			builder.setVocabulary(IndexConstants.MARKER_TYPE_NAME);
			builder.setUnique_key(rs.getString("_Label_key")
					+ IndexConstants.MARKER_TYPE_NAME);

			// Check for Marker Ortholog Synonyms

			if (rs.getString("labelType").equals(IndexConstants.MARKER_SYNOYNM)
					&& rs.getString("_OrthologOrganism_key") != null) {
				if (!rs.getString("_Label_Status_key").equals("1")) {
					builder.setIsCurrent("0");

					/*
					 * Putting this in for the future, currently in the database
					 * this case doesn't exist, but it IS perfectly legal, so
					 * may as well cover it now.
					 */

					builder.setDataType(builder.getDataType() + "O");
				}

				builder.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
				builder.setDisplay_type(displayType);

				builder.setOrganism(rs.getString("_OrthologOrganism_key"));
			}

			// We want to specially label Human and Rat Ortholog Symbols

			else if (rs.getString("labelType").equals(
					IndexConstants.ORTHOLOG_SYMBOL)) {
				String organism = rs.getString("_OrthologOrganism_key");
				if (organism != null && organism.equals("2")) {
					builder.setDataType(IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
				} else if (organism != null && organism.equals("40")) {
					builder.setDataType(IndexConstants.ORTHOLOG_SYMBOL_RAT);
				} else {
					builder.setDataType(rs.getString("labelType"));
				}
				builder.setDisplay_type(displayType);
			}

			// If we have an ortholog symbol or name, set its organism
			else {

				if (rs.getString("labelType").equals(IndexConstants.ORTHOLOG_SYMBOL) || rs.getString("labelType").equals(IndexConstants.ORTHOLOG_NAME)) {
					builder.setOrganism(rs.getString("_OrthologOrganism_key"));
				}

				builder.setDataType(rs.getString("labelType"));

				if (!rs.getString("_Label_Status_key").equals("1")) {
					builder.setIsCurrent("0");

					// We want to manufacture new label types, if the status
					// shows that they are old. Looking at the database
					// the only possibly things that this can hit at the moment
					// are Marker Name and Marker Symbol

					builder.setDataType(builder.getDataType() + "O");
				}

				// Manually remove the word current from two special cases.

				if (displayType.equals("Current Symbol")) {
					displayType = "Symbol";
				}
				if (displayType.equals("Current Name")) {
					displayType = "Name";
				}
				builder.setDisplay_type(displayType);

			}

			// Add the document to the stack

			documentStore.push(builder.getDocument());
			builder.clear();
			rs.next();
		}

		// Clean up

		rs.close();
		log.info("Done Marker Labels!");

	}

	/**
	 * Gather Vocab terms, non AD. This method goes each vocabulary one at a
	 * time, passing them onto another method for processing. This allows the
	 * sql that we use for each vocabulary type to move independently.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerms() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Since all of this SQL is marker related, we do a join to the
		// vocab annotated count cache, and only bring back vocabulary
		// records that have markers annotated to them or their children.

		log.info("Collecting EMAPS Terms");

		// Collect all emaps vocabulary terms, where the term is not obsolete.
		String EMAPS_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 91"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'EMAPS'";

		doVocabTerm(EMAPS_TERM_KEY);

//		log.info("Collecting EMAPA Terms");
//
//		// Collect all emapa vocabulary terms, where the term is not obsolete.
//		String EMAPA_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
//				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
//				+ " where tv.isObsolete != 1 and tv._Vocab_key = 90"
//				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
//				+ " 'EMAPA'";
//
//		doVocabTerm(EMAPA_TERM_KEY);

		log.info("Collecting GO Terms");

		// Collect all go vocabulary terms, where the term is not obsolete.

		String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 4"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'GO/Marker'";

		doVocabTerm(GO_TERM_KEY);

		log.info("Collecting MP Terms");

		// Collect all mp vocabulary terms, where the term is not obsolete.

		String MP_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 5"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'Mammalian Phenotype/Genotype'";

		doVocabTerm(MP_TERM_KEY);

		log.info("Collecting InterPro Terms");

		// Collect all interpro vocabulary terms, where the term is not
		// obsolete.

		String INTERPRO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 8"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'InterPro/Marker'";

		doVocabTerm(INTERPRO_TERM_KEY);

		log.info("Collecting PIRSF Terms");

		// Collect all pirsf vocabulary terms, where the term is not obsolete.

		String PIRSF_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_annot_count_cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 46"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'PIRSF/Marker'";

		doVocabTerm(PIRSF_TERM_KEY);

		log.info("Collecting OMIM Terms");

		// Collect all omim vocabulary terms, where the term is not obsolete.

		String OMIM_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 44"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'OMIM/Genotype'";

		doVocabTerm(OMIM_TERM_KEY);

		log.info("Collecting OMIM/Human Terms");

		// Collect all omim/human terms where the term is not obsolete.

		String OMIM_HUMAN_TERM_KEY = "select tv._Term_key, tv.term, '"
				+ IndexConstants.OMIM_ORTH_TYPE_NAME + "' as vocabName"
				+ " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
				+ " where tv.isObsolete != 1 and tv._Vocab_key = 44"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType = "
				+ "'OMIM/Human Marker'";

		doVocabTerm(OMIM_HUMAN_TERM_KEY);

		log.info("Done collecting all non AD vocab terms");

	}

	/**
	 * Gather the vocab synonyms, non AD. Each vocabulary type is done
	 * separately. This allows the SQL used in gathering them to move
	 * independently.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonyms() throws SQLException, InterruptedException {

		// SQL for this Subsection
		// Since this is a marker related index, only bring back vocab items
		// that have markers actually annotated to them, or their children.

		log.info("Collecintg EMAPS Synonyms");

		// Gather up all the emaps synonyms, where the term is not obsolete.

		String EMAPS_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 91 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'EMAPS'";

		doVocabSynonym(EMAPS_SYN_KEY);

//		log.info("Collecintg EMAPA Synonyms");
//
//		// Gather up all the emapa synonyms, where the term is not obsolete.
//
//		String EMAPA_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
//				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
//				+ " Voc_Annot_count_cache vacc"
//				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
//				+ " and tv._Vocab_key = 90 and s._MGIType_key = 13 "
//				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
//				+ " 'EMAPA'";
//
//		doVocabSynonym(EMAPA_SYN_KEY);

		log.info("Collecintg GO Synonyms");

		// Gather up all the go synonyms, where the term is not obsolete.

		String GO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 4 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'GO/Marker'";

		doVocabSynonym(GO_SYN_KEY);

		log.info("Collecintg MP Synonyms");

		// Gather up all the MP synonyms, where the term is not obsolete.

		String MP_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key " + "from VOC_Term_View tv, MGI_Synonym s,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 5 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'Mammalian Phenotype/Genotype'";

		doVocabSynonym(MP_SYN_KEY);

		log.info("Collecintg OMIM Synonyms");

		// Gather up all the omim non human synonyms, where the term is not
		// obsolete.

		String OMIM_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
				+ " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s, "
				+ "Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'OMIM/Genotype'";

		doVocabSynonym(OMIM_SYN_KEY);

		log.info("Collecintg OMIM/Human Synonyms");

		// Gather up all the omim human synyonms, where the term is not
		// obsolete.

		String OMIM_HUMAN_SYN_KEY = "select tv._Term_key, s.synonym, '"
				+ IndexConstants.OMIM_ORTH_TYPE_NAME
				+ "' as vocabName, s._Synonym_key"
				+ " from VOC_Term_View tv, MGI_Synonym s, "
				+ "Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
				+ "and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'OMIM/Human Marker'";

		doVocabSynonym(OMIM_HUMAN_SYN_KEY);
	}

	/**
	 * Gather the vocab notes/definitions, non AD. Once again these are done in
	 * sequence, which allows the SQL for each subtype to move independently.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabNotes() throws SQLException, InterruptedException {

		// SQL for this subsection, please note that the order by clause is
		// important for these sql statements.

		// Also since this is a marker related index only bring back notes for
		// terms who have annotations to markers.

		log.info("Collecting GO Notes/Definitions");

		// Gather up all the go notes, where the term is not obsolete, in
		// sequence number order.

		String GO_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
				+ " from VOC_Term_View tv, VOC_text t,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = t._Term_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 4"
				+ " and tv._Term_key = vacc._Term_key"
				+ " and vacc.annotType = 'GO/Marker'"
				+ " order by tv._Term_key, t.sequenceNum";

		doVocabNote(GO_NOTE_KEY);

		log.info("Collecting MP Notes/Definitions");

		// Gather up all the mp notes, where the terms is not obsolete, in
		// sequence number order.

		String MP_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
				+ " from VOC_Term_View tv, VOC_text t,"
				+ " Voc_Annot_count_cache vacc"
				+ " where tv._Term_key = t._Term_key and tv.isObsolete != 1"
				+ " and tv._Vocab_key = 5"
				+ " and tv._Term_key = vacc._Term_key and vacc.annotType ="
				+ " 'Mammalian Phenotype/Genotype'"
				+ " order by tv._Term_key, t.sequenceNum";

		doVocabNote(MP_NOTE_KEY);
	}

	/**
	 * Gather the AD Vocab Terms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabADTerm() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Also since this is a marker related index only bring back notes for
		// terms who have annotations to markers.

		log.info("Collecting AD Terms");

		// Gather all AD vocabulary terms. Ignore top level terms (those
		// whose parent key is null)

		String VOC_AD_TERM_KEY = "select s._Structure_key, s._Stage_key, "
				+ "s.printName, 'AD' as vocabName"
				+ " from GXD_Structure s, VOC_Annot_Count_Cache vacc"
				+ " where s._Parent_key is not null and vacc.annotType = 'AD'"
				+ " and vacc._Term_key = s._Structure_key";

		// Gather the data

		ResultSet rs_ad_term = executor.executeMGD(VOC_AD_TERM_KEY);
		rs_ad_term.next();

		log.info("Time taken gather AD vocab terms result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_ad_term.isAfterLast()) {

			// For AD specifically we are adding in multiple ways for something
			// to match inexactly.
			// We are also directly manipulating the data in the gatherer,
			// which is NOT the normal pattern.

			builder.setData("TS" + rs_ad_term.getString("_Stage_key") + " " + rs_ad_term.getString("printName").replaceAll(";", " "));
			builder.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": " + rs_ad_term.getString("printName").replaceAll(";", "; "));
			builder.setDb_key(rs_ad_term.getString("_Structure_key"));
			builder.setUnique_key(rs_ad_term.getString("_Structure_key") + rs_ad_term.getString("vocabName"));
			builder.setVocabulary(rs_ad_term.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_ad_term.getString("vocabName")));
			builder.setDataType(IndexConstants.VOCAB_TERM);
			documentStore.push(builder.getDocument());
			// Transformed version, w/o TS
			builder.setData(rs_ad_term.getString("printName").replaceAll(";"," "));
			documentStore.push(builder.getDocument());

			builder.clear();
			rs_ad_term.next();
		}

		// Clean up
		log.info("Done AD Vocab Terms!");
		rs_ad_term.close();

	}

	/**
	 * Gather the AD Vocab Synonyms.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabADSynonym() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Also since this is a marker related index only bring back notes for
		// terms who have annotations to markers.

		log.info("Collecting AD Synonyms");

		// Gather AD synonyms, ignoring top level terms (those who's parent
		// key == null)

		String VOC_AD_SYN_KEY = "select s._Structure_key, sn.Structure, "
				+ "'AD' as vocabName"
				+ " from GXD_Structure s, GXD_StructureName sn,"
				+ " VOC_Annot_Count_Cache vacc"
				+ " where s._parent_key is not null"
				+ " and s._Structure_key = sn._Structure_key and"
				+ " s._StructureName_key != sn._StructureName_key"
				+ " and vacc.annotType='AD' and vacc._Term_key ="
				+ " s._Structure_key";

		// Gather the data

		ResultSet rs_ad_syn = executor.executeMGD(VOC_AD_SYN_KEY);
		rs_ad_syn.next();

		log.info("Time taken gather AD vocab synonym result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_ad_syn.isAfterLast()) {

			builder.setData(rs_ad_syn.getString("Structure"));
			builder.setRaw_data(rs_ad_syn.getString("Structure"));
			builder.setDb_key(rs_ad_syn.getString("_Structure_key"));
			builder.setUnique_key(rs_ad_syn.getString("_Structure_key") + rs_ad_syn.getString("Structure") + rs_ad_syn.getString("vocabName"));
			builder.setVocabulary(rs_ad_syn.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_ad_syn.getString("vocabName")));
			builder.setDataType(IndexConstants.VOCAB_SYNONYM);

			// Place the document onto the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_ad_syn.next();
		}

		// Clean up

		log.info("Done AD Vocab Synonyms!");
		rs_ad_syn.close();
	}

	/**
	 * Gather the allele labels.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAlleleNomen() throws SQLException, InterruptedException {

		// SQL for this Subsection

		log.info("Collecting Allele Nomenclature");

		// Gather up allele nomenclature
		//
		// Includes: current allele symbols, names, and synonyms
		// Excludes: all data for wild-type alleles

		String ALLELE_NOMEN_KEY = "select distinct aa._Allele_key, "
				+ "al.label, al.labelType, al.labelTypeName"
				+ " from all_label al, ALL_Allele aa"
				+ " where al._Allele_key ="
				+ " aa._Allele_key and al._Label_Status_key != 0 "
				+ " and aa.isWildType != 1";

		// Gather the data

		ResultSet rs = executor.executeMGD(ALLELE_NOMEN_KEY);
		rs.next();

		log.info("Time taken gather allele nomenclature result set "
				+ executor.getTiming());

		// Parse it

		while (!rs.isAfterLast()) {

			builder.setData(rs.getString("label"));
			builder.setRaw_data(rs.getString("label"));
			builder.setDb_key(rs.getString("_Allele_key"));
			builder.setUnique_key(rs.getString("_Allele_key") + rs.getString("label") + rs.getString("labelType") + IndexConstants.ALLELE_TYPE_NAME);
			builder.setVocabulary(IndexConstants.ALLELE_TYPE_NAME);
			builder.setDataType(rs.getString("labelType"));
			builder.setDisplay_type(providerMap.get(rs.getString("labelType")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs.next();
		}

		// Clean up

		rs.close();
		log.info("Done Allele Nomenclature!");

	}

	/**
	 * Generic Term Gatherer, called by the specific vocab Term Methods, with
	 * the exception of AD, which uses different column names.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabTerm(String sql) throws SQLException, InterruptedException {

		// Gather the data

		ResultSet rs_term = executor.executeMGD(sql);
		

		log.info("Time taken gather vocab term result set: " + executor.getTiming());

		// Parse it

		while (rs_term.next()) {

			builder.setData(rs_term.getString("term"));
			builder.setRaw_data(rs_term.getString("term"));
			builder.setDb_key(rs_term.getString("_Term_key"));
			builder.setUnique_key(rs_term.getString("_Term_key") + rs_term.getString("vocabName"));
			builder.setVocabulary(rs_term.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_term.getString("vocabName")));
			builder.setDataType(IndexConstants.VOCAB_TERM);

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();

		}

		// Clean up
		rs_term.close();

	}

	/**
	 * Gather the vocab synonyms, non AD.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doVocabSynonym(String sql) throws SQLException, InterruptedException {

		// Gather the data

		ResultSet rs_syn = executor.executeMGD(sql);


		log.info("Time taken gather vocab synonym result set: " + executor.getTiming());

		// Parse it

		while (rs_syn.next()) {

			builder.setData(rs_syn.getString("synonym"));
			builder.setRaw_data(rs_syn.getString("synonym"));
			builder.setDb_key(rs_syn.getString("_Term_key"));
			builder.setUnique_key(rs_syn.getString("_Synonym_key") + IndexConstants.VOCAB_SYNONYM + rs_syn.getString("vocabName"));
			builder.setVocabulary(rs_syn.getString("vocabName"));
			builder.setDisplay_type(providerMap.get(rs_syn.getString("vocabName")));
			builder.setDataType(IndexConstants.VOCAB_SYNONYM);

			// Place the document on the stock.

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

	private void doVocabNote(String sql) throws SQLException,
			InterruptedException {

		ResultSet rs_note = executor.executeMGD(sql);
		rs_note.next();

		log.info("Time taken gather vocab notes/definition result set: "
				+ executor.getTiming());

		// Parse it

		int place = -1;

		// Since notes are compound rows in the database, we have to
		// construct the searchable field.

		while (!rs_note.isAfterLast()) {
			if (place != rs_note.getInt(1)) {
				if (place != -1) {

					// Place the document on the stack.

					documentStore.push(builder.getDocument());
					builder.clear();
				}
				builder.setDb_key(rs_note.getString("_Term_key"));
				builder.setUnique_key(rs_note.getString("_Term_key") + IndexConstants.VOCAB_NOTE + rs_note.getString("vocabName"));
				builder.setVocabulary(rs_note.getString("vocabName"));
				builder.setDisplay_type(providerMap.get(rs_note.getString("vocabName")));
				builder.setDataType(IndexConstants.VOCAB_NOTE);
				place = rs_note.getInt("_Term_key");
			}
			builder.appendData(rs_note.getString("note"));
			builder.appendRaw_data(rs_note.getString("note"));
			rs_note.next();
		}

		log.info("Done Vocab Notes/Definitions!");
	}
}

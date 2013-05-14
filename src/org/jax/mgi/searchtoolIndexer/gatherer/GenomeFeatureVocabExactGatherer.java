package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureVocabExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up the vocabulary term information
 * that is used when returning items to the marker bucket.  We specifically
 * exclude vocabulary terms that have no annotations to markers.
 *
 * This information is then used to populate the markerVocabExact index.
 *
 * @author mhall
 *
 * @has An instance of the IndexCfg object, which is used to setup this object.
 *
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts
 * parsing through its result set. For each record, we generate a Lucene
 * document, and place it on the shared stack.
 *
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class GenomeFeatureVocabExactGatherer extends DatabaseGatherer {

    // Class Variables

    private GenomeFeatureVocabExactLuceneDocBuilder builder =
        new GenomeFeatureVocabExactLuceneDocBuilder();

    private HashMap<String, String> providerMap = new HashMap<String, String>();

    public GenomeFeatureVocabExactGatherer(IndexCfg config) {
        super(config);

        providerMap.put(IndexConstants.VOCAB_TERM, "Term");
        providerMap.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
        providerMap.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
        providerMap.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
        providerMap.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
        providerMap.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
        providerMap.put(IndexConstants.GO_TYPE_NAME, "Function");
        providerMap.put(IndexConstants.AD_TYPE_NAME, "Expression");
    }

    public void runLocal() throws Exception {
            doVocabTerms();
            doVocabSynonyms();
            doVocabNotes();
            doVocabADTerm();
            doVocabADSynonym();
    }

    /**
     * Gather the non AD Vocab Terms.
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabTerms() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting GO terms!");

        // Collect go terms that are related to markers and are not obsolete.

        String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and"
                + " vacc.annotType = 'GO/Marker'";

        doVocabTerm(GO_TERM_KEY);

        log.info("Collecting MP Terms");

        // Collect mp terms that are related to markers and are not obsolete.

        String MP_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 5"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'Mammalian Phenotype/Genotype'";

        doVocabTerm(MP_TERM_KEY);

        log.info("Collecting Interpro Terms");

        String INTERPRO_TERM_KEY = "select tv._Term_key, tv.term,"
                + " tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 8"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'InterPro/Marker'";

        doVocabTerm(INTERPRO_TERM_KEY);

        log.info("Collecting PIRSF Terms");

        // Collect pirsf terms that are related to markers and are not
        // obsolete.

        String PIRSF_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 46"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'PIRSF/Marker'";

        doVocabTerm(PIRSF_TERM_KEY);

        log.info("Collecting OMIM Terms");

        // Collect omim/non-human terms that are related to markers and are
        // not obsolete.

        String OMIM_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";

        doVocabTerm(OMIM_TERM_KEY);

        log.info("Collecting OMIM/Human Terms");

        // Collect omim/human terms that are related to markers and are not
        // obsolete.

        String OMIM_HUMAN_TERM_KEY = "select tv._Term_key, tv.term," +
        		" '"+IndexConstants.OMIM_ORTH_TYPE_NAME+"' as vocabName"+
                " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"+
                " where tv.isObsolete != 1 and tv._Vocab_key = 44"+
                " and tv._Term_key = vacc._Term_key and vacc.annotType" +
                " = 'OMIM/Human Marker'";

        doVocabTerm(OMIM_HUMAN_TERM_KEY);

        log.info("Done collecting All Vocab Terms!");

    }

    /**
     * Gather the non ad vocab synonyms.
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabSynonyms() throws SQLException, InterruptedException {

        // SQL for this Subsection

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

        doVocabSynonym(GO_SYN_KEY);

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

        doVocabSynonym(MP_SYN_KEY);

        log.info("Collecitng OMIM Synonyms");

        // Collect omim/non-human synonyms that are related to markers and are
        // not obsolete.

        String OMIM_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";

        doVocabSynonym(OMIM_SYN_KEY);

        log.info("Collecting OMIM/Human Synonyms");

        // Collect omim/human synonyms that are related to markers and are not
        // obsolete.

        String OMIM_HUMAN_SYN_KEY = "select tv._Term_key, s.synonym, '"
                + IndexConstants.OMIM_ORTH_TYPE_NAME + "' as vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Human Marker'";

        doVocabSynonym(OMIM_HUMAN_SYN_KEY);

        log.info("Done collecting All Vocab Non AD Synonyms!");

    }

    /**
     * Gather the non ad vocab notes/definitions. Please note that this is a
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

        doVocabNote(GO_NOTE_KEY);

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

        doVocabNote (MP_NOTE_KEY);

        log.info("Done collecting all Vocab Non AD Notes/Definitions");

    }

    /**
     * Gather the AD Vocab Terms
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabADTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting AD Terms");

        // Gather up the ad terms that are related to markers.
        // Please note that we are specifically excluding top level terms
        // (terms that have no parent)

        String VOC_AD_TERM_KEY = "select s._Structure_key, s._Stage_key, "
                + "s.printName, 'AD' as vocabName"
                + " from GXD_Structure s, VOC_Annot_Count_Cache vacc"
                + " where s._Parent_key is not null and vacc.annotType = 'AD'"
                + " and vacc._Term_key = s._Structure_key";

        // Gather the data

        ResultSet rs_ad_term = executor.executeMGD(VOC_AD_TERM_KEY);
        rs_ad_term.next();

        log.debug("Time taken gather result set: "
                + executor.getTiming());

        // Parse it

        while (!rs_ad_term.isAfterLast()) {
            // Add in the TS form w/o space
            builder.setData("TS" + rs_ad_term.getString("_Stage_key") + ":"
                    + rs_ad_term.getString("printName"));
            builder.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            builder.setUnique_key(rs_ad_term.getString("_Structure_key")
                    + rs_ad_term.getString("vocabName"));
            builder.setDb_key(rs_ad_term.getString("_Structure_key"));
            builder.setVocabulary(rs_ad_term.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_TERM);
            builder.setDisplay_type(providerMap.get(rs_ad_term.getString("vocabName")));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            // Untransformed version, w/ space
            builder.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName"));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            // Untransformed version, w/o TS
            builder.setData(rs_ad_term.getString("printName"));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            // Transformed version, w/ TS
            builder.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());

            builder.clear();
            rs_ad_term.next();
        }

        // Clean up

        rs_ad_term.close();
        log.info("Done collecting Vocab AD Terms!");

    }

    /**
     * Gather the AD Vocab Synonyms
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabADSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting AD Synonyms");

        // Gather up the ad synonyms that are related to markers.  Please note
        // that we are specifically excluding top level terms (terms that have
        // no parent defined)

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

        log.debug("Time taken gather result set: "
                + executor.getTiming());

        // Parse it

        while (!rs_ad_syn.isAfterLast()) {
            builder.setData(rs_ad_syn.getString("Structure"));
            builder.setRaw_data(rs_ad_syn.getString("Structure"));
            builder.setDb_key(rs_ad_syn.getString("_Structure_key"));
            builder.setVocabulary(rs_ad_syn.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_SYNONYM);
            builder.setUnique_key(rs_ad_syn.getString("_Structure_key")
                    + rs_ad_syn.getString("Structure")
                    + rs_ad_syn.getString("vocabName"));
            builder.setDisplay_type(providerMap.get(rs_ad_syn.getString("vocabName")));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            builder.clear();
            rs_ad_syn.next();
        }

        // Clean up

        rs_ad_syn.close();
        log.info("Done collecting Vocab AD Synonyms!");

    }

    /**
     * Gather the non AD Vocab Terms.
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabTerm(String sql)
        throws SQLException, InterruptedException {

        ResultSet rs_non_ad_term = executor.executeMGD(sql);
        rs_non_ad_term.next();

        log.debug("Time taken gather result set: "
                + executor.getTiming());

        // Parse it

        while (!rs_non_ad_term.isAfterLast()) {

            builder.setVocabulary(rs_non_ad_term.getString("vocabName"));
            builder.setData(rs_non_ad_term.getString("term"));
            builder.setRaw_data(rs_non_ad_term.getString("term"));
            builder.setDb_key(rs_non_ad_term.getString("_Term_key"));
            builder.setDataType(IndexConstants.VOCAB_TERM);
            builder.setUnique_key(rs_non_ad_term.getString("_Term_key")
                    + rs_non_ad_term.getString("vocabName"));
            builder.setDisplay_type(providerMap.get(rs_non_ad_term
                    .getString("vocabName")));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            builder.clear();
            rs_non_ad_term.next();
        }

        // Clean up

        rs_non_ad_term.close();

    }

    /**
     * Gather the non ad vocab synonyms.
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabSynonym(String sql)
        throws SQLException, InterruptedException {

        // Gather the Data

        ResultSet rs_non_ad_syn = executor.executeMGD(sql);
        rs_non_ad_syn.next();

        log.debug("Time taken gather result set: "
                + executor.getTiming());

        // Parse it

        while (!rs_non_ad_syn.isAfterLast()) {
            builder.setData(rs_non_ad_syn.getString("synonym"));
            builder.setRaw_data(rs_non_ad_syn.getString("synonym"));
            builder.setDb_key(rs_non_ad_syn.getString("_Term_key"));
            builder.setVocabulary(rs_non_ad_syn.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_SYNONYM);
            builder.setUnique_key(rs_non_ad_syn.getString("_Synonym_key")
                    + IndexConstants.VOCAB_SYNONYM
                    + rs_non_ad_syn.getString("vocabName"));
            builder.setDisplay_type(providerMap.get(rs_non_ad_syn
                    .getString("vocabName")));

            // Place the document on the stack.

            documentStore.push(builder.getDocument());
            builder.clear();
            rs_non_ad_syn.next();
        }

        // Clean up

        rs_non_ad_syn.close();

    }

    /**
     * Gather the non ad vocab notes/definitions. Please note that this is a
     * compound field, and thus its parser has special handling.
     *
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabNote(String sql)
        throws SQLException, InterruptedException {

        // Gather the data.

        ResultSet rs_non_ad_note = executor.executeMGD(sql);
        rs_non_ad_note.next();

        log.debug("Time taken gather result set: "
                + executor.getTiming());

        // Parse it

        int place = -1;

        while (!rs_non_ad_note.isAfterLast()) {
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
                builder.setUnique_key(rs_non_ad_note.getString("_Term_key")
                        + IndexConstants.VOCAB_NOTE
                        + rs_non_ad_note.getString("vocabName"));
                builder.setDisplay_type(providerMap.get(rs_non_ad_note
                        .getString("vocabName")));
                place = rs_non_ad_note.getInt("_Term_key");
            }
            builder.appendData(rs_non_ad_note.getString("note"));
            rs_non_ad_note.next();
        }

        // Clean up

        rs_non_ad_note.close();

    }
}

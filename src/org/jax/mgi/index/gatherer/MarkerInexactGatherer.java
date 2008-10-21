package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerInexactLuceneDocBuilder;
import org.jax.mgi.shr.config.Configuration;

import QS_Commons.IndexConstants;

/**
 * This class is responsible to gather up anything we might want to match a marker on
 * inexactly.  Meaning where punctuation is non important, and prefix searching will be
 * allowed.
 * 
 * @author mhall
 *
 * @has A hashmap, which is used to translated from shortened codes ("MN") to
 * human readable words ("Marker Name")
 * 
 * A MarkerInexactLuceneDocBuilder, which encapsulates the data for this type, 
 * and produces lucene documents.
 * 
 * @does Upon being started it runs through a group of methods that gather up
 * all of the different items we want searched in this index.
 * 
 * Each method behaves as follows:
 * 
 * Gather its data, parse it, add the document to the stack, repeat until finished,
 * clean up the result set, and exit.
 * 
 * After each method has completed, we clean up the overall jdbc connection, set gathering
 * to complete and exit.
 *
 */

public class MarkerInexactGatherer extends AbstractGatherer {

    // Class Variables

    private Date                          writeStart;
    private Date                          writeEnd;

    // Instantiate the single doc builder that this object will use.

    private MarkerInexactLuceneDocBuilder markerInexact = new MarkerInexactLuceneDocBuilder();

    private Logger log = Logger.getLogger(MarkerInexactGatherer.class.getName());
    
    public static HashMap<String, String> hm            = new HashMap<String, String>();

    public MarkerInexactGatherer(Configuration config) {
        super(config);

        /*
         * Please notice that three of these hashmap values come from strings
         * that are not contained in the index constants. This is because the
         * code that we use for them is generated in this objects Doc Builder.
         */

        hm.put(IndexConstants.MARKER_SYMBOL, "Marker Symbol");
        hm.put(IndexConstants.MARKER_NAME, "Marker Name");
        hm.put(IndexConstants.MARKER_SYNOYNM, "Marker Synonym");
        hm.put(IndexConstants.ALLELE_SYMBOL, "Allele Symbol");
        hm.put(IndexConstants.ALLELE_NAME, "Allele Name");
        hm.put(IndexConstants.ALLELE_SYNONYM, "Allele Synonym");
        hm.put(IndexConstants.ORTHOLOG_SYMBOL, "Ortholog Symbol");
        hm.put(IndexConstants.ORTHOLOG_NAME, "Ortholog Name");
        hm.put(IndexConstants.ORTHOLOG_SYNONYM, "Ortholog Synonym");
        hm.put(IndexConstants.GO_TYPE_NAME, "Gene Ontology");
        hm.put("Mammalian Phenotype", "Phenotype");
        hm.put("PIR Superfamily", "Protein Family");
        hm.put("InterPro Domains", "Protein Domain");
        hm.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
        hm.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
        hm.put(IndexConstants.GO_TYPE_NAME, "Function");
        hm.put(IndexConstants.AD_TYPE_NAME, "Expression");
    }

    public void run() {
        try {

            doMarkerLabels();

            doVocabTerm();

            doVocabSynonym(); 

            doVocabNotes();

            doVocabADTerm();

            doVocabADSynonym();
            
            doAlleleSynonym();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Gather the marker labels.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doMarkerLabels() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting Marker Labels");
        
        String GEN_MARKER_LABEL = "select ml._Marker_key, ml.label, "
                + "ml.labelType, ml._OrthologOrganism_key, "
                + "ml._Label_Status_key, ml.labelTypeName, ml._Label_key" + " from MRK_Label ml, MRK_Marker m"
                + " where ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2 ";
                //+ "and _Label_Status_key = 1";

        // Gather the data

        writeStart = new Date();

        ResultSet rs = execute(GEN_MARKER_LABEL);
        rs.next();

        writeEnd = new Date();

        log.info("Time taken gather marker label result set "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        String displayType = "";
        
        

        while (!rs.isAfterLast()) {
            
            displayType = initCap(rs.getString("labelTypeName"));
            
            markerInexact.setData(rs.getString("label"));
            markerInexact.setRaw_data(rs.getString("label"));
            markerInexact.setDb_key(rs.getString("_Marker_key"));
            markerInexact.setVocabulary(IndexConstants.MARKER_TYPE_NAME);
            markerInexact.setUnique_key(rs.getString("_Label_key") + IndexConstants.MARKER_TYPE_NAME);

            if (rs.getString("labelType").equals(IndexConstants.MARKER_SYNOYNM)
                    && rs.getString("_OrthologOrganism_key") != null) {
                if (!rs.getString("_Label_Status_key").equals("1")) {
                    markerInexact.setIsCurrent("0");
                    
                    /*
                     * Putting this in for the future, currently in the database
                     * this case doesn't exist, but it IS perfectly legal, so may
                     * as well cover it now.
                     */
                    
                    markerInexact.setDataType(markerInexact.getDataType()+"O");
                }

                markerInexact.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
                markerInexact.setDisplay_type(displayType);

                markerInexact
                        .setOrganism(rs.getString("_OrthologOrganism_key"));
            } 
            else if (rs.getString("labelType").equals(IndexConstants.ORTHOLOG_SYMBOL)) {
                String organism = rs.getString("_OrthologOrganism_key");
                if (organism != null && organism.equals("2")) {
                    markerInexact.setDataType(IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
                }
                else if (organism != null && organism.equals("44")) {
                    markerInexact.setDataType(IndexConstants.ORTHOLOG_SYMBOL_RAT);
                }
                else {
                    markerInexact.setDataType(rs.getString("labelType"));  
                }
                markerInexact.setDisplay_type(displayType);
            }
            else {

                if (rs.getString("labelType").equals(
                        IndexConstants.ORTHOLOG_SYMBOL)
                        || rs.getString("labelType").equals(
                                IndexConstants.ORTHOLOG_NAME)) {
                    markerInexact.setOrganism(rs
                            .getString("_OrthologOrganism_key"));
                }

                markerInexact.setDataType(rs.getString("labelType"));

                if (!rs.getString("_Label_Status_key").equals("1")) {
                    markerInexact.setIsCurrent("0");
                    
                    // We want to manufacture new label types, if the status
                    // shows that they are old.  Looking at the database
                    // the only possibly things that this can hit at the moment
                    // are Marker Name and Marker Symbol
                    
                    markerInexact.setDataType(markerInexact.getDataType()+"O");
                }
                
                
                if (displayType.equals("Current Symbol")) {
                    displayType = "Symbol";
                }
                if (displayType.equals("Current Name")) {
                    displayType = "Name";
                }
                markerInexact
                .setDisplay_type(displayType);

                }

            sis.push(markerInexact.getDocument());
            markerInexact.clear();
            rs.next();
        }

        // Clean up

        rs.close();
        log.info("Done Marker Labels!");

    }

    /**
     * Gather Vocab terms, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting GO Terms");
        
        String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'GO/Marker'";

        doVocabTerm(GO_TERM_KEY);

        log.info("Collecting MP Terms");
        
        String MP_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 5"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'Mammalian Phenotype/Genotype'";

        doVocabTerm(MP_TERM_KEY);

        log.info("Collecting InterPro Terms");
        
        String INTERPRO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 8"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'InterPro/Marker'";

        doVocabTerm(INTERPRO_TERM_KEY);

        log.info("Collecting PIRSF Terms");
        
        String PIRSF_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 46"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'PIRSF/Marker'";

        doVocabTerm(PIRSF_TERM_KEY);

        log.info("Collecting OMIM Terms");
        
        String OMIM_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'OMIM/Genotype'";

        doVocabTerm(OMIM_TERM_KEY);

        log.info("Collecting OMIM/Human Terms");
        
        String OMIM_HUMAN_TERM_KEY = "select tv._Term_key, tv.term, '"+IndexConstants.OMIM_ORTH_TYPE_NAME+"' as vocabName"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'OMIM/Human Marker'";

        doVocabTerm(OMIM_HUMAN_TERM_KEY);
        
        log.info("Done collecting all non AD vocab terms");
        
    }

    /**
     * Gather the vocab synonyms, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecintg GO Synonyms");
        
        String GO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName, s._Synonym_key"
            + " from VOC_Term_View tv, MGI_Synonym s, Voc_Annot_count_cache vacc"
            + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
            + " and tv._Vocab_key = 4 and s._MGIType_key = 13 "
            + "and tv._Term_key = vacc._Term_key and vacc.annotType = 'GO/Marker'";
        
        doVocabSynonym(GO_SYN_KEY);
        
        log.info("Collecintg MP Synonyms");
        
        String MP_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName, s._Synonym_key"
            + " from VOC_Term_View tv, MGI_Synonym s, Voc_Annot_count_cache vacc"
            + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
            + " and tv._Vocab_key = 5 and s._MGIType_key = 13 "
            + "and tv._Term_key = vacc._Term_key and vacc.annotType = 'Mammalian Phenotype/Genotype'";
        
        doVocabSynonym(MP_SYN_KEY);
        
        log.info("Collecintg OMIM Synonyms");
        
        String OMIM_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName, s._Synonym_key"
            + " from VOC_Term_View tv, MGI_Synonym s, Voc_Annot_count_cache vacc"
            + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
            + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
            + "and tv._Term_key = vacc._Term_key and vacc.annotType = 'OMIM/Genotype'";
        
        doVocabSynonym(OMIM_SYN_KEY);
        
        log.info("Collecintg OMIM/Human Synonyms");
        
        String OMIM_HUMAN_SYN_KEY = "select tv._Term_key, s.synonym, '"+IndexConstants.OMIM_ORTH_TYPE_NAME+"' as vocabName, s._Synonym_key"
            + " from VOC_Term_View tv, MGI_Synonym s, Voc_Annot_count_cache vacc"
            + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
            + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
            + "and tv._Term_key = vacc._Term_key and vacc.annotType = 'OMIM/Human Marker'";
        
        doVocabSynonym(OMIM_HUMAN_SYN_KEY);
    }

    /**
     * Gather the vocab notes/definitions, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabNotes() throws SQLException, InterruptedException {

        // SQL for this subsection, please note that the order by clause is
        // important
        // for this sql statement.

        log.info("Collecting GO Notes/Definitions");
        
        String GO_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t, Voc_Annot_count_cache vacc"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + " and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'GO/Marker'"
                + " order by tv._Term_key, t.sequenceNum";

        doVocabNotes(GO_NOTE_KEY);
        
        log.info("Collecting MP Notes/Definitions");
        
        String MP_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
            + " from VOC_Term_View tv, VOC_text t, Voc_Annot_count_cache vacc"
            + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
            + " and tv._Vocab_key = 5"
            + " and tv._Term_key = vacc._Term_key and vacc.annotType = 'Mammalian Phenotype/Genotype'"
            + " order by tv._Term_key, t.sequenceNum";

        doVocabNotes(MP_NOTE_KEY);
    }

    /**
     * Gather the AD Vocab Terms.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabADTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting AD Terms");
        
        String VOC_AD_TERM_KEY = "select s._Structure_key, s._Stage_key, "
                + "s.printName, 'AD' as vocabName" + " from GXD_Structure s"
                + " where s._Parent_key != null";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_ad_term = execute(VOC_AD_TERM_KEY);
        rs_ad_term.next();

        writeEnd = new Date();

        log.info("Time taken gather AD vocab terms result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_term.isAfterLast()) {

            // For AD specifically we are adding in multiple ways for something
            // to match inexactly.

            markerInexact.setData("TS" + rs_ad_term.getString("_Stage_key")
                    + " "
                    + rs_ad_term.getString("printName").replaceAll(";", " "));
            markerInexact.setRaw_data("TS" + rs_ad_term.getString("_Stage_key")
                    + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            markerInexact.setDb_key(rs_ad_term.getString("_Structure_key"));
            markerInexact.setUnique_key(rs_ad_term.getString("_Structure_key")+rs_ad_term.getString("vocabName"));
            markerInexact.setVocabulary(rs_ad_term.getString("vocabName"));
            markerInexact.setDisplay_type(hm.get(rs_ad_term
                    .getString("vocabName")));
            markerInexact.setDataType(IndexConstants.VOCAB_TERM);
            sis.push(markerInexact.getDocument());
            // Transformed version, w/o TS
            markerInexact.setData(rs_ad_term.getString("printName").replaceAll(
                    ";", " "));
            sis.push(markerInexact.getDocument());

            markerInexact.clear();
            rs_ad_term.next();
        }

        // Clean up
        log.info("Done AD Vocab Terms!");
        rs_ad_term.close();

    }

    /**
     * Gather the AD Vocab Synonyms.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabADSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        log.info("Collecting GO Synonyms");
        
        String VOC_AD_SYN_KEY = "select s._Structure_key, sn.Structure, "
                + "'AD' as vocabName"
                + " from dbo.GXD_Structure s, GXD_StructureName sn"
                + " where s._parent_key != null"
                + " and s._Structure_key = sn._Structure_key and "
                + "s._StructureName_key != sn._StructureName_key";

        // Gather the data
        
        writeStart = new Date();

        ResultSet rs_ad_syn = execute(VOC_AD_SYN_KEY);
        rs_ad_syn.next();

        writeEnd = new Date();

        log.info("Time taken gather AD vocab synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        while (!rs_ad_syn.isAfterLast()) {

            markerInexact.setData(rs_ad_syn.getString("Structure"));
            markerInexact.setRaw_data(rs_ad_syn.getString("Structure"));
            markerInexact.setDb_key(rs_ad_syn.getString("_Structure_key"));
            markerInexact.setUnique_key(rs_ad_syn.getString("_Structure_key")+rs_ad_syn.getString("Structure")+rs_ad_syn.getString("vocabName"));
            markerInexact.setVocabulary(rs_ad_syn.getString("vocabName"));
            markerInexact.setDisplay_type(hm.get(rs_ad_syn.getString("vocabName")));
            markerInexact.setDataType(IndexConstants.VOCAB_SYNONYM);
            sis.push(markerInexact.getDocument());
            markerInexact.clear();
            rs_ad_syn.next();
        }

        // Clean up
        
        log.info("Done AD Vocab Synonyms!");
        rs_ad_syn.close();
    }

    /**
     * Gather the marker labels.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doAlleleSynonym() throws SQLException, InterruptedException {
    
        // SQL for this Subsection
    
        log.info("Collecting Allele Synonyms");
        
        String ALLELE_SYNONYM_KEY = "select distinct gag._Marker_key, al.label, al.labelType, al.labelTypeName"+
 " from all_label al, GXD_AlleleGenotype gag"+
 " where al.labelType = 'AY' and al._Allele_key = gag._Allele_key"+
 " and al._Label_Status_key != 0";
    
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs = execute(ALLELE_SYNONYM_KEY);
        rs.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather allele synonym result set "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it 
    
        while (!rs.isAfterLast()) {
            
            markerInexact.setData(rs.getString("label"));
            markerInexact.setRaw_data(rs.getString("label"));
            markerInexact.setDb_key(rs.getString("_Marker_key"));
            markerInexact.setUnique_key(rs.getString("_Marker_key")+rs.getString("label") + IndexConstants.MARKER_TYPE_NAME);
            markerInexact.setVocabulary(IndexConstants.MARKER_TYPE_NAME); 
            markerInexact.setDataType(rs.getString("labelType"));
            markerInexact.setDisplay_type(hm.get(rs.getString("labelType")));
    
            sis.push(markerInexact.getDocument());
            markerInexact.clear();
            rs.next();
        }
    
        // Clean up
    
        rs.close();
        log.info("Done Allele Synonyms!");
    
    }

    /**
     * Generic Term Gatherer, called by the specific vocab Term Methods, with the exception 
     * of AD, which uses different column names.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabTerm(String sql) throws SQLException, InterruptedException {
    
   
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs_term = execute(sql);
        rs_term.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab term result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_term.isAfterLast()) {
    
            markerInexact.setData(rs_term.getString("term"));
            markerInexact.setRaw_data(rs_term.getString("term"));
            markerInexact.setDb_key(rs_term.getString("_Term_key"));
            markerInexact.setUnique_key(rs_term.getString("_Term_key")+rs_term.getString("vocabName"));
            markerInexact.setVocabulary(rs_term.getString("vocabName"));
            markerInexact.setDisplay_type(hm.get(rs_term.getString("vocabName")));
            markerInexact.setDataType(IndexConstants.VOCAB_TERM);
            sis.push(markerInexact.getDocument());
            markerInexact.clear();
            rs_term.next();
        }
    
        // Clean up
        rs_term.close();
    
    }

    /**
     * Gather the vocab synonyms, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabSynonym(String sql) throws SQLException, InterruptedException {
        
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs_syn = execute(sql);
        rs_syn.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_syn.isAfterLast()) {
    
            markerInexact.setData(rs_syn.getString("synonym"));
            markerInexact.setRaw_data(rs_syn.getString("synonym"));
            markerInexact.setDb_key(rs_syn.getString("_Term_key"));
            markerInexact.setUnique_key(rs_syn.getString("_Synonym_key")+IndexConstants.VOCAB_SYNONYM+rs_syn.getString("vocabName"));
            markerInexact.setVocabulary(rs_syn.getString("vocabName"));
            markerInexact
                    .setDisplay_type(hm.get(rs_syn.getString("vocabName")));
            markerInexact.setDataType(IndexConstants.VOCAB_SYNONYM);
            sis.push(markerInexact.getDocument());
            markerInexact.clear();
            rs_syn.next();
        }
    
        // Clean up
    
        log.info("Done Vocab Synonyms!");
        rs_syn.close();
    }

    /**
     * Gather the vocab notes/definitions, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabNotes(String sql) throws SQLException, InterruptedException {
        
        writeStart = new Date();
    
        ResultSet rs_note = execute(sql);
        rs_note.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab notes/definition result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        int place = -1;
    
        // Since notes are compound rows in the database, we have to
        // contruct the searchable field.
    
        while (!rs_note.isAfterLast()) {
            if (place != rs_note.getInt(1)) {
                if (place != -1) {
                    markerInexact.setRaw_data(markerInexact.getData());
                    sis.push(markerInexact.getDocument());
                    markerInexact.clear();
                }
                markerInexact.setDb_key(rs_note.getString("_Term_key"));
                markerInexact.setUnique_key(rs_note.getString("_Term_key")+IndexConstants.VOCAB_NOTE+rs_note.getString("vocabName"));
                markerInexact.setVocabulary(rs_note.getString("vocabName"));
                markerInexact.setDisplay_type(hm.get(rs_note
                        .getString("vocabName")));
                markerInexact.setDataType(IndexConstants.VOCAB_NOTE);
                place = rs_note.getInt("_Term_key");
            }
            markerInexact.appendData(rs_note.getString("note"));
            rs_note.next();
        }
    
        log.info("Done Vocab Notes/Definitions!");
    }
}

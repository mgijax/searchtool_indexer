package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerInexactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible to gather up anything we might want to match a 
 * marker on inexactly.  Meaning where punctuation is non important, 
 * and prefix searching will be allowed.
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
 * Gather its data, parse it, add the document to the stack, repeat until 
 * finished, clean up the result set, and exit.
 * 
 * After each method has completed, we clean up the overall jdbc connection,
 *  set gathering to complete and exit.
 *
 */

public class MarkerInexactGatherer extends AbstractGatherer {

    // Class Variables

    private Date                          writeStart;
    private Date                          writeEnd;

    // Instantiate the single doc builder that this object will use.

    private MarkerInexactLuceneDocBuilder mildb =
        new MarkerInexactLuceneDocBuilder();

    private Logger log =
        Logger.getLogger(MarkerInexactGatherer.class.getName());
    
    public static HashMap<String, String> hm =
        new HashMap<String, String>();

    public MarkerInexactGatherer(IndexCfg config) {
        super(config);

        /*
         * Please notice that three of these hashmap values are from a slightly
         * different source than the others.  Interpro, pirsf and mp can have
         * a longer type name in some tables.  For this dataset we need to use
         * the longer names.
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
        hm.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
        hm.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Family");
        hm.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Domain");
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
        
        String MARKER_LABEL_KEY = "select ml._Marker_key, ml.label, "
                + "ml.labelType, ml._OrthologOrganism_key, "
                + "ml._Label_Status_key, ml.labelTypeName, ml._Label_key" 
                + " from MRK_Label ml, MRK_Marker m"
                + " where ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2 ";

        // Gather the data

        writeStart = new Date();

        ResultSet rs = execute(MARKER_LABEL_KEY);
        rs.next();

        writeEnd = new Date();

        log.info("Time taken gather marker label result set "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        String displayType = "";
        
        

        while (!rs.isAfterLast()) {
            
            displayType = initCap(rs.getString("labelTypeName"));
            
            mildb.setData(rs.getString("label"));
            mildb.setRaw_data(rs.getString("label"));
            mildb.setDb_key(rs.getString("_Marker_key"));
            mildb.setVocabulary(IndexConstants.MARKER_TYPE_NAME);
            mildb.setUnique_key(rs.getString("_Label_key") 
                    + IndexConstants.MARKER_TYPE_NAME);

            // Check for Marker Ortholog Synonyms
            
            if (rs.getString("labelType").equals(IndexConstants.MARKER_SYNOYNM)
                    && rs.getString("_OrthologOrganism_key") != null) {
                if (!rs.getString("_Label_Status_key").equals("1")) {
                    mildb.setIsCurrent("0");
                    
                    /*
                     * Putting this in for the future, currently in the 
                     * database this case doesn't exist, but it IS perfectly
                     * legal, so may as well cover it now.
                     */
                    
                    mildb.setDataType(mildb.getDataType()+"O");
                }

                mildb.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
                mildb.setDisplay_type(displayType);

                mildb.setOrganism(rs.getString("_OrthologOrganism_key"));
            } 
            
            // We want to specially label Human and Rat Ortholog Symbols
            
            else if (rs.getString("labelType").equals(
                    IndexConstants.ORTHOLOG_SYMBOL)) {
                String organism = rs.getString("_OrthologOrganism_key");
                if (organism != null && organism.equals("2")) {
                    mildb.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
                }
                else if (organism != null && organism.equals("40")) {
                    mildb.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_RAT);
                }
                else {
                    mildb.setDataType(rs.getString("labelType"));  
                }
                mildb.setDisplay_type(displayType);
            }
            
            // If we have an ortholog symbol or name, set its organism
            else {

                if (rs.getString("labelType").equals(
                        IndexConstants.ORTHOLOG_SYMBOL)
                        || rs.getString("labelType").equals(
                                IndexConstants.ORTHOLOG_NAME)) {
                    mildb.setOrganism(rs
                            .getString("_OrthologOrganism_key"));
                }

                mildb.setDataType(rs.getString("labelType"));

                if (!rs.getString("_Label_Status_key").equals("1")) {
                    mildb.setIsCurrent("0");
                    
                    // We want to manufacture new label types, if the status
                    // shows that they are old.  Looking at the database
                    // the only possibly things that this can hit at the moment
                    // are Marker Name and Marker Symbol
                    
                    mildb.setDataType(mildb.getDataType()+"O");
                }
                
                // Manually remove the word current from two special cases.
                
                if (displayType.equals("Current Symbol")) {
                    displayType = "Symbol";
                }
                if (displayType.equals("Current Name")) {
                    displayType = "Name";
                }
                mildb.setDisplay_type(displayType);

                }

            // Add the document to the stack
            
            sis.push(mildb.getDocument());
            mildb.clear();
            rs.next();
        }

        // Clean up

        rs.close();
        log.info("Done Marker Labels!");

    }

    /**
     * Gather Vocab terms, non AD.  This method goes each vocabulary one at a 
     * time, passing them onto another method for processing.  This allows the
     * sql that we use for each vocabulary type to move independently.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        // Since all of this SQL is marker related, we do a join to the 
        // vocab annotated count cache, and only bring back vocabulary 
        // records that have markers annotated to them or thier children.

        log.info("Collecting GO Terms");
        
        String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'GO/Marker'";

        doVocabTerm(GO_TERM_KEY);

        log.info("Collecting MP Terms");
        
        String MP_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 5"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'Mammalian Phenotype/Genotype'";

        doVocabTerm(MP_TERM_KEY);

        log.info("Collecting InterPro Terms");
        
        String INTERPRO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 8"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'InterPro/Marker'";

        doVocabTerm(INTERPRO_TERM_KEY);

        log.info("Collecting PIRSF Terms");

        String PIRSF_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 46"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'PIRSF/Marker'";

        doVocabTerm(PIRSF_TERM_KEY);

        log.info("Collecting OMIM Terms");

        String OMIM_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";

        doVocabTerm(OMIM_TERM_KEY);

        log.info("Collecting OMIM/Human Terms");
        
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
     * Gather the vocab synonyms, non AD.  Each vocabulary type is done
     * separately.  This allows the SQL used in gathering them to move 
     * independently.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection
        // Since this is a marker related index, only bring back vocab items
        // that have markers actually annotated to them, or their children.
        
        log.info("Collecintg GO Synonyms");
        
        String GO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 4 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'GO/Marker'";
        
        doVocabSynonym(GO_SYN_KEY);
        
        log.info("Collecintg MP Synonyms");
        
        String MP_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key " + "from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 5 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'Mammalian Phenotype/Genotype'";
        
        doVocabSynonym(MP_SYN_KEY);
        
        log.info("Collecintg OMIM Synonyms");
        
        String OMIM_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s, "
                + "Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";
        
        doVocabSynonym(OMIM_SYN_KEY);
        
        log.info("Collecintg OMIM/Human Synonyms");
        
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
     * Gather the vocab notes/definitions, non AD.
     * Once again these are done in sequence, which allows the SQL for each 
     * subtype to move independently.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabNotes() throws SQLException, InterruptedException {

        // SQL for this subsection, please note that the order by clause is
        // important for these sql statements.
        
        // Also since this is a marker related index only bring back notes for
        // terms who have annotations to markers.

        log.info("Collecting GO Notes/Definitions");
        
        String GO_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key"
                + " and vacc.annotType = 'GO/Marker'"
                + " order by tv._Term_key, t.sequenceNum";

        doVocabNotes(GO_NOTE_KEY);
        
        log.info("Collecting MP Notes/Definitions");
        
        String MP_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 5"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'Mammalian Phenotype/Genotype'"
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

        // Also since this is a marker related index only bring back notes for
        // terms who have annotations to markers.
        
        log.info("Collecting AD Terms");
        
        String VOC_AD_TERM_KEY = "select s._Structure_key, s._Stage_key, "
                + "s.printName, 'AD' as vocabName"
                + " from GXD_Structure s, VOC_Annot_Count_Cache vacc"
                + " where s._Parent_key != null and vacc.annotType = 'AD'"
                + " and vacc._Term_key = s._Structure_key";

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

            mildb.setData("TS" + rs_ad_term.getString("_Stage_key") + " "
                    + rs_ad_term.getString("printName").replaceAll(";", " "));
            mildb.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            mildb.setDb_key(rs_ad_term.getString("_Structure_key"));
            mildb.setUnique_key(rs_ad_term.getString("_Structure_key")
                    +rs_ad_term.getString("vocabName"));
            mildb.setVocabulary(rs_ad_term.getString("vocabName"));
            mildb.setDisplay_type(hm.get(rs_ad_term
                    .getString("vocabName")));
            mildb.setDataType(IndexConstants.VOCAB_TERM);
            sis.push(mildb.getDocument());
            // Transformed version, w/o TS
            mildb.setData(rs_ad_term.getString("printName").replaceAll(
                    ";", " "));
            sis.push(mildb.getDocument());

            mildb.clear();
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
        
        // Also since this is a marker related index only bring back notes for
        // terms who have annotations to markers.
        
        log.info("Collecting AD Synonyms");
        
        String VOC_AD_SYN_KEY = "select s._Structure_key, sn.Structure, "
                + "'AD' as vocabName"
                + " from dbo.GXD_Structure s, GXD_StructureName sn,"
                + " VOC_Annot_Count_Cache vacc"
                + " where s._parent_key != null"
                + " and s._Structure_key = sn._Structure_key and"
                + " s._StructureName_key != sn._StructureName_key"
                + " and vacc.annotType='AD' and vacc._Term_key ="
                + " s._Structure_key";

        // Gather the data
        
        writeStart = new Date();

        ResultSet rs_ad_syn = execute(VOC_AD_SYN_KEY);
        rs_ad_syn.next();

        writeEnd = new Date();

        log.info("Time taken gather AD vocab synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        while (!rs_ad_syn.isAfterLast()) {

            mildb.setData(rs_ad_syn.getString("Structure"));
            mildb.setRaw_data(rs_ad_syn.getString("Structure"));
            mildb.setDb_key(rs_ad_syn.getString("_Structure_key"));
            mildb.setUnique_key(rs_ad_syn.getString("_Structure_key")
                    +rs_ad_syn.getString("Structure")
                    +rs_ad_syn.getString("vocabName"));
            mildb.setVocabulary(rs_ad_syn.getString("vocabName"));
            mildb.setDisplay_type(hm.get(
                    rs_ad_syn.getString("vocabName")));
            mildb.setDataType(IndexConstants.VOCAB_SYNONYM);
            
            // Place the document onto the stack.
            
            sis.push(mildb.getDocument());
            mildb.clear();
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
        
        String ALLELE_SYNONYM_KEY = "select distinct gag._Marker_key, " +
        		"al.label, al.labelType, al.labelTypeName"+
        		" from all_label al, GXD_AlleleGenotype gag"+
        		" where al.labelType = 'AY' and al._Allele_key =" +
        		" gag._Allele_key and al._Label_Status_key != 0";
    
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs = execute(ALLELE_SYNONYM_KEY);
        rs.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather allele synonym result set "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it 
    
        while (!rs.isAfterLast()) {
            
            mildb.setData(rs.getString("label"));
            mildb.setRaw_data(rs.getString("label"));
            mildb.setDb_key(rs.getString("_Marker_key"));
            mildb.setUnique_key(rs.getString("_Marker_key")
                    +rs.getString("label") + IndexConstants.MARKER_TYPE_NAME);
            mildb.setVocabulary(IndexConstants.MARKER_TYPE_NAME); 
            mildb.setDataType(rs.getString("labelType"));
            mildb.setDisplay_type(hm.get(rs.getString("labelType")));
    
            // Place the document on the stack.
            
            sis.push(mildb.getDocument());
            mildb.clear();
            rs.next();
        }
    
        // Clean up
    
        rs.close();
        log.info("Done Allele Synonyms!");
    
    }

    /**
     * Generic Term Gatherer, called by the specific vocab Term Methods,
     *  with the exception of AD, which uses different column names.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabTerm(String sql) 
        throws SQLException, InterruptedException {
    
   
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs_term = execute(sql);
        rs_term.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab term result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_term.isAfterLast()) {
    
            mildb.setData(rs_term.getString("term"));
            mildb.setRaw_data(rs_term.getString("term"));
            mildb.setDb_key(rs_term.getString("_Term_key"));
            mildb.setUnique_key(rs_term.getString("_Term_key")
                    +rs_term.getString("vocabName"));
            mildb.setVocabulary(rs_term.getString("vocabName"));
            mildb.setDisplay_type(hm.get(
                    rs_term.getString("vocabName")));
            mildb.setDataType(IndexConstants.VOCAB_TERM);
            
            // Place the document on the stack.
            
            sis.push(mildb.getDocument());
            mildb.clear();
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
    
    private void doVocabSynonym(String sql) 
        throws SQLException, InterruptedException {
        
        // Gather the data
    
        writeStart = new Date();
    
        ResultSet rs_syn = execute(sql);
        rs_syn.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_syn.isAfterLast()) {
    
            mildb.setData(rs_syn.getString("synonym"));
            mildb.setRaw_data(rs_syn.getString("synonym"));
            mildb.setDb_key(rs_syn.getString("_Term_key"));
            mildb.setUnique_key(rs_syn.getString("_Synonym_key")
                    +IndexConstants.VOCAB_SYNONYM
                    +rs_syn.getString("vocabName"));
            mildb.setVocabulary(rs_syn.getString("vocabName"));
            mildb
                    .setDisplay_type(hm.get(rs_syn.getString("vocabName")));
            mildb.setDataType(IndexConstants.VOCAB_SYNONYM);
            
            // Place the document on the stock.
            
            sis.push(mildb.getDocument());
            mildb.clear();
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
    
    private void doVocabNotes(String sql) 
        throws SQLException, InterruptedException {
        
        writeStart = new Date();
    
        ResultSet rs_note = execute(sql);
        rs_note.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather vocab notes/definition result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        int place = -1;
    
        // Since notes are compound rows in the database, we have to
        // construct the searchable field.
    
        while (!rs_note.isAfterLast()) {
            if (place != rs_note.getInt(1)) {
                if (place != -1) {
                    mildb.setRaw_data(mildb.getData());
                    
                    // Place the document on the stack.
                    
                    sis.push(mildb.getDocument());
                    mildb.clear();
                }
                mildb.setDb_key(rs_note.getString("_Term_key"));
                mildb.setUnique_key(rs_note.getString("_Term_key")
                        +IndexConstants.VOCAB_NOTE
                        +rs_note.getString("vocabName"));
                mildb.setVocabulary(rs_note.getString("vocabName"));
                mildb.setDisplay_type(hm.get(rs_note
                        .getString("vocabName")));
                mildb.setDataType(IndexConstants.VOCAB_NOTE);
                place = rs_note.getInt("_Term_key");
            }
            mildb.appendData(rs_note.getString("note"));
            rs_note.next();
        }
    
        log.info("Done Vocab Notes/Definitions!");
    }
}

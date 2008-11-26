package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.index.MakeIndex;
import org.jax.mgi.index.luceneDocBuilder.VocabExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

public class MarkerVocabExactGatherer extends AbstractGatherer {

    // Class Variables

    private Date                       writeStart;
    private Date                       writeEnd;

    private Logger log = Logger.getLogger(MakeIndex.class.getName());
    
    private VocabExactLuceneDocBuilder veldb =
        new VocabExactLuceneDocBuilder();

    private HashMap<String, String> hm = new HashMap<String, String>();

    public MarkerVocabExactGatherer(IndexCfg config) {
        super(config);

        hm.put(IndexConstants.VOCAB_TERM, "Term");
        hm.put(IndexConstants.MP_DATABASE_TYPE, "Phenotype");
        hm.put(IndexConstants.PIRSF_DATABASE_TYPE, "Protein Family");
        hm.put(IndexConstants.INTERPRO_DATABASE_TYPE, "Protein Domain");
        hm.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
        hm.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
        hm.put(IndexConstants.GO_TYPE_NAME, "Function");
        hm.put(IndexConstants.AD_TYPE_NAME, "Expression");
    }

    public void run() {
        try {

            doVocabTerm();

            doVocabSynonym();

            doVocabNote();

            doVocabADTerm();

            doVocabADSynonym();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Gather the non AD Vocab Terms.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabTerm() throws SQLException, InterruptedException {
    
        // SQL for this Subsection
        
        log.info("Collecting GO terms!");
        
        String GO_TERM_KEY = "select tv._Term_key, tv.term, tv.vocabName"
                + " from VOC_Term_View tv, VOC_annot_count_cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and"
                + " vacc.annotType = 'GO/Marker'";    
                
        doVocabTerm(GO_TERM_KEY);
                
        log.info("Collecting MP Terms");
        
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

    private void doVocabSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Collecting GO Synonyms");

        String GO_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 4 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType"
                + " = 'GO/Marker'";
        
        doVocabSynonym(GO_SYN_KEY);
        
        log.info("Collecting MP Synonyms");
        
        String MP_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 5 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType"
                + " = 'Mammalian Phenotype/Genotype'";
        
        doVocabSynonym(MP_SYN_KEY);
        
        log.info("Collecitng OMIM Synonyms");
        
        String OMIM_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName,"
                + " s._Synonym_key" + " from VOC_Term_View tv, MGI_Synonym s,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key = 44 and s._MGIType_key = 13 "
                + "and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";
        
        doVocabSynonym(OMIM_SYN_KEY);
        
        log.info("Collecting OMIM/Human Synonyms");
        
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

    private void doVocabNote() throws SQLException, InterruptedException {

        // SQL for this subsection
        
        log.info("Collecting GO Notes/Definitions");

        String GO_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t,"
                + " Voc_Annot_count_cache vacc"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + " and tv._Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'GO/Marker' order by tv._Term_key, t.sequenceNum";

        doVocabNote(GO_NOTE_KEY);
    
        log.info("Collecting MP Notes/Definitions");
        
        String MP_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
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

        log.debug("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_term.isAfterLast()) {
            // Add in the TS form w/o space
            veldb.setData("TS" + rs_ad_term.getString("_Stage_key") + ":"
                    + rs_ad_term.getString("printName"));
            veldb.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            veldb.setUnique_key(rs_ad_term.getString("_Structure_key")
                    + rs_ad_term.getString("vocabName"));
            veldb.setDb_key(rs_ad_term.getString("_Structure_key"));
            veldb.setVocabulary(rs_ad_term.getString("vocabName"));
            veldb.setDataType(IndexConstants.VOCAB_TERM);
            veldb.setDisplay_type(hm.get(rs_ad_term.getString("vocabName")));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            // Untransformed version, w/ space
            veldb.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName"));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            // Untransformed version, w/o TS
            veldb.setData(rs_ad_term.getString("printName"));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            // Transformed version, w/ TS
            veldb.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());

            veldb.clear();
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

        log.debug("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_syn.isAfterLast()) {
            veldb.setData(rs_ad_syn.getString("Structure"));
            veldb.setRaw_data(rs_ad_syn.getString("Structure"));
            veldb.setDb_key(rs_ad_syn.getString("_Structure_key"));
            veldb.setVocabulary(rs_ad_syn.getString("vocabName"));
            veldb.setDataType(IndexConstants.VOCAB_SYNONYM);
            veldb.setUnique_key(rs_ad_syn.getString("_Structure_key")
                    + rs_ad_syn.getString("Structure")
                    + rs_ad_syn.getString("vocabName"));
            veldb.setDisplay_type(hm.get(rs_ad_syn.getString("vocabName")));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            veldb.clear();
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
    
    
        writeStart = new Date();
    
        ResultSet rs_non_ad_term = execute(sql);
        rs_non_ad_term.next();
    
        writeEnd = new Date();
    
        log.debug("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_non_ad_term.isAfterLast()) {

            veldb.setVocabulary(rs_non_ad_term.getString("vocabName"));
            veldb.setData(rs_non_ad_term.getString("term"));
            veldb.setRaw_data(rs_non_ad_term.getString("term"));
            veldb.setDb_key(rs_non_ad_term.getString("_Term_key"));
            veldb.setDataType(IndexConstants.VOCAB_TERM);
            veldb.setUnique_key(rs_non_ad_term.getString("_Term_key")
                    + rs_non_ad_term.getString("vocabName"));
            veldb.setDisplay_type(hm.get(rs_non_ad_term
                    .getString("vocabName")));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            veldb.clear();
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
    
        writeStart = new Date();
    
        ResultSet rs_non_ad_syn = execute(sql);
        rs_non_ad_syn.next();
    
        writeEnd = new Date();
    
        log.debug("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        while (!rs_non_ad_syn.isAfterLast()) {
            veldb.setData(rs_non_ad_syn.getString("synonym"));
            veldb.setRaw_data(rs_non_ad_syn.getString("synonym"));
            veldb.setDb_key(rs_non_ad_syn.getString("_Term_key"));
            veldb.setVocabulary(rs_non_ad_syn.getString("vocabName"));
            veldb.setDataType(IndexConstants.VOCAB_SYNONYM);
            veldb.setUnique_key(rs_non_ad_syn.getString("_Synonym_key")
                    + IndexConstants.VOCAB_SYNONYM
                    + rs_non_ad_syn.getString("vocabName"));
            veldb.setDisplay_type(hm.get(rs_non_ad_syn
                    .getString("vocabName")));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            veldb.clear();
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
    
        writeStart = new Date();
    
        ResultSet rs_non_ad_note = execute(sql);
        rs_non_ad_note.next();
    
        writeEnd = new Date();
    
        log.debug("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
    
        int place = -1;
    
        while (!rs_non_ad_note.isAfterLast()) {
            if (place != rs_non_ad_note.getInt("_Term_key")) {
                if (place != -1) {
                    veldb.setRaw_data(veldb.getData());
                    
                    // Place the document on the stack.
                    
                    sis.push(veldb.getDocument());
                    veldb.clear();
                }
    
                veldb.setDb_key(rs_non_ad_note.getString("_Term_key"));
                veldb.setVocabulary(rs_non_ad_note.getString("vocabName"));
                veldb.setDataType(IndexConstants.VOCAB_NOTE);
                veldb.setUnique_key(rs_non_ad_note.getString("_Term_key")
                        + IndexConstants.VOCAB_NOTE
                        + rs_non_ad_note.getString("vocabName"));
                veldb.setDisplay_type(hm.get(rs_non_ad_note
                        .getString("vocabName")));
                place = rs_non_ad_note.getInt("_Term_key");
            }
            veldb.appendData(rs_non_ad_note.getString("note"));
            rs_non_ad_note.next();
        }
    
        // Clean up
    
        rs_non_ad_note.close();
    
    }
}

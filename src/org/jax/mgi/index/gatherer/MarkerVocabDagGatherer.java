package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerVocabDagLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;

import QS_Commons.IndexConstants;

public class MarkerVocabDagGatherer extends AbstractGatherer {

    // Class Variables

    private Date                         writeStart;
    private Date                         writeEnd;

    // Instantiate the single VocabDisplay Lucene doc builder.

    private MarkerVocabDagLuceneDocBuilder vocabDisplay = new MarkerVocabDagLuceneDocBuilder();

    private Logger log = Logger.getLogger(MarkerVocabDagGatherer.class.getName());
    
    HashMap <String, String> hm = new HashMap <String, String> ();

    public MarkerVocabDagGatherer(IndexCfg config) {
        super(config);
        
        hm.put(IndexConstants.GO_TYPE_NAME, "Gene Ontology");
        hm.put("Mammalian Phenotype", "Phenotype");
        hm.put("PIR Superfamily", "Protein Family");
        hm.put("InterPro Domains", "Protein Domain");
        hm.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
        hm.put(IndexConstants.GO_TYPE_NAME, "Function");
        hm.put(IndexConstants.AD_TYPE_NAME, "Expression");
        hm.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
        
    }

    public void run() {
        try {

            doNonADVocab();

            doADVocab();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Gather up all of the display information for non AD vocab terms.  
     * We do this seperately for each vocabulary type, so if requirements 
     * change for any specific type, we can change them independantly.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doNonADVocab() throws SQLException, InterruptedException {

        log.info("Collecting Non AD Dag Information");
        
        // Since this is a compound object, the order by clauses are important.
        
        log.info("Collecting GO Records!");
        
        // GO
        
        String GO_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
            + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
            + " where tv.isObsolete != 1 and tv._Vocab_key = 4"
            + " and vacc.annotType = 'GO/Marker' and vacc._Term_key = tv._Term_key"
            + " order by _Term_key";

        String GO_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
        		"from VOC_Marker_Cache"
            + " where annotType = 'GO/Marker'" + 
            " order by _Term_key";


        String GO_DAG_KEY = "select dc._AncestorObject_key, dc._DescendentObject_key"
            + " from DAG_Closure dc, VOC_Annot_Count_Cache vacc, "
            + " VOC_Term vt, VOC_AnnotType vat"
            + " where dc._MGIType_key = 13"
            + " and dc._DescendentObject_key = vacc._Term_key"
            + " and vt._Term_key =dc._DescendentObject_key "
            + " and vt._Vocab_key = vat._Vocab_key and vat.name = vacc.annotType " +
            		"and vacc.annotType = 'GO/Marker'"
            + " order by dc._AncestorObject_key, dc._DescendentObject_key";
        
        doNonADVocab(GO_VOC_KEY, GO_MARKER_DISPLAY_KEY, GO_DAG_KEY);
        
        log.info("Collecting MP Records!");

        String MP_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
                + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where tv.isObsolete != 1 and tv._Vocab_key = 5"
                + " and vacc.annotType = 'Mammalian Phenotype/Genotype' "
                + "and vacc._Term_key = tv._Term_key" + " order by _Term_key";
        
        String MP_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
                "from VOC_Marker_Cache"
            + " where annotType = 'Mammalian Phenotype/Genotype'" + 
            " order by _Term_key";


        String MP_DAG_KEY = "select dc._AncestorObject_key, dc._DescendentObject_key"
            + " from DAG_Closure dc, VOC_Annot_Count_Cache vacc, "
            + " VOC_Term vt, VOC_AnnotType vat"
            + " where dc._MGIType_key = 13"
            + " and dc._DescendentObject_key = vacc._Term_key"
            + " and vt._Term_key =dc._DescendentObject_key "
            + " and vt._Vocab_key = vat._Vocab_key and vat.name = vacc.annotType " +
                    "and vacc.annotType = 'Mammalian Phenotype/Genotype'"
            + " order by dc._AncestorObject_key, dc._DescendentObject_key";
        
        doNonADVocab(MP_VOC_KEY, MP_MARKER_DISPLAY_KEY, MP_DAG_KEY);
        
        // Interpro
        
        log.info("Collecting Interpro Records!");
        
        String INTERPRO_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
            + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
            + " where tv.isObsolete != 1 and tv._Vocab_key = 8"
            + " and vacc.annotType = 'InterPro/Marker' and vacc._Term_key = tv._Term_key"
            + " order by _Term_key";

        String INTERPRO_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
                "from VOC_Marker_Cache"
            + " where annotType = 'InterPro/Marker'" + 
            " order by _Term_key";
        
        doNonADVocab(INTERPRO_VOC_KEY, INTERPRO_MARKER_DISPLAY_KEY);
        
       // PIRSF
        
        log.info("Collecting PIRSF Records!");
        
        String PIRSF_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
            + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
            + " where tv.isObsolete != 1 and tv._Vocab_key = 46" 
            + " and vacc.annotType = 'PIRSF/Marker' and vacc._Term_key = tv._Term_key"
            + " order by _Term_key";

        String PIRSF_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
                "from VOC_Marker_Cache"
            + " where annotType = 'PIRSF/Marker'" + 
            " order by _Term_key";
        
        doNonADVocab(PIRSF_VOC_KEY, PIRSF_MARKER_DISPLAY_KEY);
        
        // OMIM
        
        log.info("Collecting OMIM Records!");
        
        String OMIM_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
            + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
            + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
            + " and vacc.annotType = 'OMIM/Genotype' and vacc._Term_key = tv._Term_key"
            + " order by _Term_key";

        String OMIM_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
                "from VOC_Marker_Cache"
            + " where annotType = 'OMIM/Genotype'" + 
            " order by _Term_key";
        
        doNonADVocab(OMIM_VOC_KEY, OMIM_MARKER_DISPLAY_KEY);
        
        // OMIM Human Orthologs
        
        String OMIM_HUMAN_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, '"+IndexConstants.OMIM_ORTH_TYPE_NAME+"' as vocabName"
            + " FROM dbo.VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
            + " where tv.isObsolete != 1 and tv._Vocab_key = 44"
            + " and vacc.annotType = 'OMIM/Human Marker' and vacc._Term_key = tv._Term_key"
            + " order by _Term_key";

        String OMIM_HUMAN_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key " +
                "from VOC_Marker_Cache"
            + " where annotType = 'OMIM/Human Marker'" + 
            " order by _Term_key";
        
        doNonADVocab(OMIM_HUMAN_VOC_KEY, OMIM_HUMAN_MARKER_DISPLAY_KEY);
        
        log.info("Done Collecting Non AD Dag Information Records!");
        
    }

    /**
     * Gather up all of the AD Term's display information, please note that while similar to
     * non ad terms, AD is special in that it doesn't posses Accession ID's.  
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doADVocab() throws SQLException, InterruptedException {

        // SQL For this Subsection

        log.info("Collecting AD Dag Information!");

        String GEN_AD_KEY = "SELECT distinct s._Structure_key, 'TS' + convert(VARCHAR, s._Stage_key) +': '+ s.printName as PrintName2, 'AD' as VocabName"
                + " FROM dbo.GXD_Structure s, GXD_StructureName sn, VOC_Annot_Count_Cache vac"
                + " where s._parent_key != null"
                + " and s._Structure_key = sn._Structure_key"
                + " and s._Structure_key = vac._Term_key"
                + " and vac.annotType = 'AD'" + " order by _Structure_key";

        String AD_VOC_DAG_KEY = "select gsc._Structure_key, gsc._Descendent_key"
                + " from GXD_StructureClosure gsc, VOC_Annot_Count_Cache vacc"
                + " where gsc._Descendent_key = vacc._Term_key"
                + " and vacc.annotType='AD'"
                + " order by gsc._Structure_key, gsc._Descendent_key";

        String VOC_AD_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key  from VOC_Marker_Cache"
                + " where annotType = 'AD'" + " order by _Term_key";

        writeStart = new Date();
        
        ResultSet rs_ad = execute(GEN_AD_KEY);

        ResultSet ad_marker_display_rs = execute(VOC_AD_MARKER_DISPLAY_KEY);
        ad_marker_display_rs.next();

        ResultSet ad_child_rs = execute(AD_VOC_DAG_KEY);
        ad_child_rs.next();
        
        writeEnd = new Date();

        log.info("Time taken gather AD Display result sets: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Since these are compound documents, we need to keep track of the document we are on.

        int place = -1;

        while (rs_ad.next()) {
            
            // Have we found a new document?
            
            if (place != rs_ad.getInt("_Structure_key")) {
                
                // If so, and its not the first document, add the current document
                // to the stack.
                
                if (place != -1) {
                    sis.push(vocabDisplay.getDocument());
                    vocabDisplay.clear();
                }
                
                // Populate the basic document information.
                
                vocabDisplay.setDb_key(rs_ad.getString("_Structure_key"));
                vocabDisplay.setContents(rs_ad.getString("PrintName2"));
                vocabDisplay.setVocabulary(rs_ad.getString("VocabName"));
                vocabDisplay.setTypeDisplay(hm.get(rs_ad.getString("VocabName")));
                vocabDisplay.setUnique_key(rs_ad.getString("_Structure_key")+rs_ad.getString("vocabName"));

                // Set the document place, when this changes we know we are on a
                // new document.
                
                place = rs_ad.getInt("_Structure_key");

                // Grab the markers directly associated to these terms.
                
                while (!ad_marker_display_rs.isAfterLast()
                        && ad_marker_display_rs.getInt("_Term_key") <= place) {
                    if (ad_marker_display_rs.getInt("_Term_key") == place) {
                        vocabDisplay.appendGene_ids(ad_marker_display_rs
                                .getString("_Marker_key"));
                    }
                    ad_marker_display_rs.next();
                }
                
                // Grab the terms that are decendants of this term, by term key order.
                
                while (!ad_child_rs.isAfterLast()
                        && ad_child_rs.getInt("_Structure_key") <= place) {
                    if (ad_child_rs.getInt("_Structure_key") == place) {
                        vocabDisplay.appendChild_ids(ad_child_rs.getString("_Descendent_key"));
                    }
                    // child_place = ad_child_rs.getInt(1);
                    ad_child_rs.next();
                }
            }
        }

        // Push the last document onto the stack, the one the loop kicked out on.
        
        sis.push(vocabDisplay.getDocument());
        
        // Clean up
        
        rs_ad.close();
        ad_marker_display_rs.close();
        ad_child_rs.close();
        
        log.info("Done Collecting AD Dag information!");
    }

    /**
     * Gather up all of the display information for non AD vocab terms. 
     * This method assumes that it will be given three sql strings,
     * one to define the term itself, one to define the set of markers to assign
     * to terms for a given vocabulary, and one to define the children of the given 
     * term.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doNonADVocab(String voc_key, String voc_marker_key, String voc_dag_key) throws SQLException, InterruptedException {
    
        
        writeStart = new Date();
    
        ResultSet rs_vocabTerm = execute(voc_key);
    
        ResultSet marker_display_rs = execute(voc_marker_key);
        marker_display_rs.next();
    
        ResultSet child_rs = execute(voc_dag_key);
        child_rs.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
        int place = -1;
    
        /*
         * These documents are compound in nature, for each document we create,
         * we are running several queries into the database This is most
         * definately the single most complicated document that we create for
         * the entire indexing process.
         * 
         */
    
        while (rs_vocabTerm.next()) {
    
            // Have we found a new document?
    
            if (place != rs_vocabTerm.getInt(1)) {
                if (place != -1) {
    
                    // We have, try to put it on the stack, waiting if the stack
                    // is busy.
                    while (sis.size() > 100000) {
                        Thread.sleep(1);
                    }
    
                    // Place the current document on the stack.
    
                    sis.push(vocabDisplay.getDocument());
    
                    // Clear the document creation object, to ready it for the
                    // next doc.
    
                    vocabDisplay.clear();
                }
    
                // Populate the document with information pertaining
                // specifically to the vocab term we are now on.
    
                vocabDisplay.setDb_key(rs_vocabTerm.getString("_Term_key"));
                vocabDisplay.setVocabulary(rs_vocabTerm.getString("vocabName"));
                vocabDisplay.setTypeDisplay(hm.get(rs_vocabTerm.getString("vocabName")));
                vocabDisplay.setAcc_id(rs_vocabTerm.getString("accID"));
                vocabDisplay.setUnique_key(rs_vocabTerm.getString("_Term_key")+rs_vocabTerm.getString("vocabName"));
    
                // Set the place to be the current terms object key, when this
                // changes we know we are on a new document.
    
                place = (int) rs_vocabTerm.getInt("_Term_key");
    
                // Find all of the genes that are directly annotated to this
                // vocabulary term, and append them into this document
    
                while (!marker_display_rs.isAfterLast()
                        && marker_display_rs.getInt("_Term_key") <= place) {
                    if (marker_display_rs.getInt("_Term_key") == place) {
                        vocabDisplay.appendGene_ids(marker_display_rs
                                .getString("_Marker_key"));
                    }
                    marker_display_rs.next();
                }
    
                // Add in all the other vocabulary terms that are children on this term
                // in term_key order.
    
                while (!child_rs.isAfterLast() && child_rs.getInt("_AncestorObject_key") <= place) {
                    if (child_rs.getInt("_AncestorObject_key") == place) {
                        vocabDisplay.appendChild_ids(child_rs.getString("_DescendentObject_key"));
                    }
                    child_rs.next();
    
                }
            }
            vocabDisplay.setContents(rs_vocabTerm.getString("term"));
        }
    
        // Add the Final Document, that the loop kicked out on.
    
        sis.push(vocabDisplay.getDocument());
        vocabDisplay.clear();
        
        // Clean up
        
        rs_vocabTerm.close();
        marker_display_rs.close();
        child_rs.close();
    }

    /**
     * Gather up all of the display information for non AD vocab terms. 
     * This method assumes that we have been passed two SQL Statements.
     * 
     * The first defines the terms for a given vocabulary.
     * The Second defines the markers to assign to those terms.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doNonADVocab(String voc_key, String voc_marker_key) throws SQLException, InterruptedException {
    
        
        writeStart = new Date();
    
        ResultSet rs_vocabTerm = execute(voc_key);
    
        ResultSet marker_display_rs = execute(voc_marker_key);
        marker_display_rs.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
        int place = -1;
    
        /*
         * These documents are compound in nature, for each document we create,
         * we are running several queries into the database This is most
         * definately the single most complicated document that we create for
         * the entire indexing process.
         * 
         */
    
        while (rs_vocabTerm.next()) {
    
            // Have we found a new document?
    
            if (place != rs_vocabTerm.getInt(1)) {
                if (place != -1) {
    
                    // We have, try to put it on the stack, waiting if the stack
                    // is busy.
                    while (sis.size() > 100000) {
                        Thread.sleep(1);
                    }
    
                    // Place the current document on the stack.
    
                    sis.push(vocabDisplay.getDocument());
    
                    // Clear the document creation object, to ready it for the
                    // next doc.
    
                    vocabDisplay.clear();
                }
    
                // Populate the document with information pertaining
                // specifically to the vocab term we are now on.
    
                vocabDisplay.setDb_key(rs_vocabTerm.getString("_Term_key"));
                vocabDisplay.setVocabulary(rs_vocabTerm.getString("vocabName"));
                vocabDisplay.setTypeDisplay(hm.get(rs_vocabTerm.getString("vocabName")));
                vocabDisplay.setAcc_id(rs_vocabTerm.getString("accID"));
                vocabDisplay.setUnique_key(rs_vocabTerm.getString("_Term_key")+rs_vocabTerm.getString("vocabName"));
    
                // Set the place to be the current terms object key, when this
                // changes we know we are on a new document.
    
                place = (int) rs_vocabTerm.getInt("_Term_key");
    
                // Find all of the genes that are directly annotated to this
                // vocabulary term, and append them into this document
    
                while (!marker_display_rs.isAfterLast()
                        && marker_display_rs.getInt("_Term_key") <= place) {
                    if (marker_display_rs.getInt("_Term_key") == place) {
                        vocabDisplay.appendGene_ids(marker_display_rs
                                .getString("_Marker_key"));
                    }
                    marker_display_rs.next();
                }
    
            }
            vocabDisplay.setContents(rs_vocabTerm.getString("term"));
        }
    
        // Add the Final Document, that the loop kicked out on.
    
        sis.push(vocabDisplay.getDocument());
        vocabDisplay.clear();
        
        // Clean up
        
        rs_vocabTerm.close();
        marker_display_rs.close();
    }

}

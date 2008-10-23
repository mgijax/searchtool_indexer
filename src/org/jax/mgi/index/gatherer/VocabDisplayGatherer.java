package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.VocabDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

public class VocabDisplayGatherer extends AbstractGatherer {

    // Class Variables

    private Date                         writeStart;
    private Date                         writeEnd;

    // Instantiate the single VocabDisplay Lucene doc builder.

    private VocabDisplayLuceneDocBuilder vocabDisplay = new VocabDisplayLuceneDocBuilder();

    private Logger log = Logger.getLogger(VocabDisplayGatherer.class.getName());
    
    HashMap <String, String> hm = new HashMap <String, String> ();
    
    // SQL Section

    public VocabDisplayGatherer(IndexCfg config) {
        super(config);
        
        hm.put(IndexConstants.GO_TYPE_NAME, "Gene Ontology");
        hm.put("Mammalian Phenotype", "Phenotype");
        hm.put("PIR Superfamily", "Protein Family");
        hm.put("InterPro Domains", "Protein Domain");
        hm.put(IndexConstants.OMIM_TYPE_NAME, "Disease");
        hm.put(IndexConstants.GO_TYPE_NAME, "Function");
        hm.put(IndexConstants.AD_TYPE_NAME, "Expression");
        
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
     * Gather up all of the display information for non AD vocab terms.  This is by far
     * the most complex thing we do in indexing, as such the code is documented inline much 
     * more carefully.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doNonADVocab() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // Since this is a compound object, the order by clauses are important.

        String GEN_VOC_KEY = "SELECT tv._Term_key, tv.term,  tv.accID, tv.vocabName"
                + " FROM dbo.VOC_Term_View tv"
                + " where tv.isObsolete != 1 and tv._Vocab_key in "
                + "(44, 4, 5, 8, 46)"
                + " order by _Term_key";

        String VOC_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key from VOC_Marker_Cache"
                + " where annotType != 'AD'" + " order by _Term_key";

        String VOC_NON_AD_MARKER_COUNT = "select _Term_key, count(_Marker_key) as marker_count"
                + " from VOC_Marker_Cache" + " where annotType !='AD'"
                + " group by _Term_key order by _Term_key";

        String VOC_NON_AD_ANNOT_COUNT = "select _Term_key, _MGIType_key, "
                + "objectCount, annotCount" + " from VOC_Annot_Count_Cache"
                + " where annotType != 'AD'" + " order by _Term_key";

        String VOC_DAG_KEY = "select _AncestorObject_key, _DescendentObject_key"
                + " from DAG_Closure"
                + " where _MGIType_key = 13"
                + " order by _AncestorObject_key, _DescendentObject_key";

        writeStart = new Date();

        ResultSet rs_vocabTerm = execute(GEN_VOC_KEY);

        ResultSet marker_display_rs = execute(VOC_MARKER_DISPLAY_KEY);
        marker_display_rs.next();

        ResultSet vocab_annot_rs = execute(VOC_NON_AD_ANNOT_COUNT);
        vocab_annot_rs.next();

        ResultSet vocab_marker_count_rs = execute(VOC_NON_AD_MARKER_COUNT);
        vocab_marker_count_rs.next();

        ResultSet child_rs = execute(VOC_DAG_KEY);
        child_rs.next();

        writeEnd = new Date();

        log.info("Time taken gather Non AD Display result sets: "
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

                // Set the annotation counts, and in the case of non human omim, the secondary object counts.

                while (!vocab_annot_rs.isAfterLast()
                        && vocab_annot_rs.getInt("_Term_key") <= place) {

                    if (!vocabDisplay.getVocabulary().equals("OMIM")
                            || (vocabDisplay.getVocabulary().equals("OMIM") && vocab_annot_rs
                                    .getString("_MGIType_key").equals("12"))) {
                        vocabDisplay.setAnnotation_object_type(vocab_annot_rs
                                .getString("_MGIType_key"));
                        vocabDisplay.setAnnotation_objects(vocab_annot_rs
                                .getString("objectCount"));
                        vocabDisplay.setAnnotation_count(vocab_annot_rs
                                .getString("annotCount"));
                    } else {
                        // log.info("Term: " + new_mp.getDb_key()
                        // + " Has a secondary annotation");
                        vocabDisplay.setSecondary_object_count(vocab_annot_rs
                                .getString("annotCount"));
                    }
                    vocab_annot_rs.next();
                }

                // Count the number of markers directly annotated to this object.

                while (!vocab_marker_count_rs.isAfterLast()
                        && vocab_marker_count_rs.getInt("_Term_key") < place) {
                    vocab_marker_count_rs.next();
                }
                if (!vocab_marker_count_rs.isAfterLast()
                        && vocab_marker_count_rs.getInt("_Term_key") == place) {
                    vocabDisplay.setMarker_count(vocab_marker_count_rs
                            .getString("marker_count"));
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
        
        log.info("Done Non AD Display Information!");
        
        rs_vocabTerm.close();
        marker_display_rs.close();
        vocab_annot_rs.close();
        vocab_marker_count_rs.close();
        child_rs.close();
    }

    /**
     * Gather up all of the AD Term's display information, please note that while similar to
     * non ad terms, AD is special in that it doesn't posses Accession ID's.  
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doADVocab() throws SQLException, InterruptedException {

        // SQL For this Subsection

/*        String VOC_AD_ANNOT_COUNT = "select _Term_key, _MGIType_key, objects, annotations"
                + " from VOC_Annot_Count_Cache" + " where name = 'AD'";
*/
        String VOC_AD_MARKER_COUNT = "select _Term_key, count(_Marker_key) as marker_count"
                + " from VOC_Marker_Cache" + " where annotType ='AD'"
                + " group by _Term_key order by _Term_key";

/*        String GEN_AD_KEY = "SELECT s._Structure_key, 'TS' + convert(VARCHAR, s._Stage_key) +': '+ s.printName as PrintName2, sn.Structure as synonym, s._StructureName_key as key1, sn._StructureName_key as key2, 'AD' as VocabName"
                + " FROM dbo.GXD_Structure s, GXD_StructureName sn"
                + " where s._parent_key != null"
                + " and s._Structure_key *= sn._Structure_key ";*/
        
/*        String GEN_AD_KEY = "SELECT distinct s._Structure_key, 'TS' + convert(VARCHAR, s._Stage_key) +': '+ " 
                + "s.printName as PrintName2, 'AD' as VocabName"
                + " FROM dbo.GXD_Structure s, GXD_StructureName sn"
                + " where s._parent_key != null"
                + " and s._Structure_key *= sn._Structure_key" 
                + " order by _Structure_key";*/
        

        String GEN_AD_KEY = "SELECT distinct s._Structure_key, 'TS' + convert(VARCHAR, s._Stage_key) +': '+ s.printName as PrintName2, 'AD' as VocabName, vac.objectCount, vac.annotCount, vac._MGIType_key"
                + " FROM dbo.GXD_Structure s, GXD_StructureName sn, VOC_Annot_Count_Cache vac"
                + " where s._parent_key != null"
                + " and s._Structure_key = sn._Structure_key"
                + " and s._Structure_key *= vac._Term_key"
                + " and vac.annotType = 'AD'" + " order by _Structure_key";

        String AD_VOC_DAG_KEY = "select _Structure_key, _Descendent_key"
                + " from GXD_StructureClosure"
                + " order by _Structure_key, _Descendent_key";

        String VOC_AD_MARKER_DISPLAY_KEY = "select distinct _Term_key, _Marker_key  from VOC_Marker_Cache"
                + " where annotType = 'AD'" + " order by _Term_key";

        writeStart = new Date();
        
        ResultSet rs_ad = execute(GEN_AD_KEY);

        ResultSet ad_marker_display_rs = execute(VOC_AD_MARKER_DISPLAY_KEY);
        ad_marker_display_rs.next();

        ResultSet vocab_marker_count_rs_ad = execute(VOC_AD_MARKER_COUNT);
        vocab_marker_count_rs_ad.next();

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
                vocabDisplay.setTypeDisplay(hm.get(rs_ad.getString("vocabName")));
                vocabDisplay.setAnnotation_object_type(rs_ad.getString("_MGIType_key"));
                vocabDisplay.setAnnotation_objects(rs_ad.getString("objectCount"));
                vocabDisplay.setAnnotation_count(rs_ad.getString("annotCount"));

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
                
                // Grab the marker counts
                
                while (!vocab_marker_count_rs_ad.isAfterLast()
                        && vocab_marker_count_rs_ad.getInt("_Term_key") < place) {
                    vocab_marker_count_rs_ad.next();
                }
                if (!vocab_marker_count_rs_ad.isAfterLast()
                        && vocab_marker_count_rs_ad.getInt("_Term_key") == place) {
                    vocabDisplay.setMarker_count(vocab_marker_count_rs_ad
                            .getString("marker_count"));
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
        
        log.info("Done AD Display Inforamtion!");
        
        rs_ad.close();
        ad_marker_display_rs.close();
        vocab_marker_count_rs_ad.close();
        ad_child_rs.close();
    }

}

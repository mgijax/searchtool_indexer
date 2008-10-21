package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.NonIDTokenLuceneDocBuilder;
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

public class NonIDTokenGatherer extends AbstractGatherer {

    // Class Variables

    private Date                          writeStart;
    private Date                          writeEnd;

    // Instantiate the single doc builder that this object will use.

    private NonIDTokenLuceneDocBuilder nonIDToken = new NonIDTokenLuceneDocBuilder();

    public static HashMap<String, String> hm            = new HashMap<String, String>();

    private Logger log = Logger.getLogger(NonIDTokenGatherer.class.getName());
    
    public NonIDTokenGatherer(Configuration config) {
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
        hm.put(IndexConstants.ORTHOLOG_SYMBOL, "Ortholog Symbol");
        hm.put(IndexConstants.ORTHOLOG_NAME, "Ortholog Name");
        hm.put(IndexConstants.ORTHOLOG_SYNONYM, "Ortholog Synonym");
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

        String GEN_MARKER_LABEL = "select ml._Marker_key, ml.label, "
                + "ml.labelType, ml._OrthologOrganism_key, "
                + "ml._Label_Status_key, ml.labelTypeName" + " from MRK_Label ml, MRK_Marker m"
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
        
        //String displayType = "";
        
        

        while (!rs.isAfterLast()) {
            
            //displayType = initCap(rs.getString("labelTypeName"));
            
            nonIDToken.setData(rs.getString("label"));

            sis.push(nonIDToken.getDocument());
            nonIDToken.clear();
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

        String VOC_TERM_KEY = "select _Term_key, term, vocabName"
                + " from VOC_Term_View"
                + " where isObsolete != 1 and _Vocab_key in (44, 4, 5, 8, 46)";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_term = execute(VOC_TERM_KEY);
        rs_term.next();

        writeEnd = new Date();

        log.info("Time taken gather vocab term result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_term.isAfterLast()) {

            nonIDToken.setData(rs_term.getString("term"));
            sis.push(nonIDToken.getDocument());
            nonIDToken.clear();
            rs_term.next();
        }

        // Clean up

        log.info("Done Vocab Terms!");
        rs_term.close();

    }

    /**
     * Gather the vocab synonyms, non AD.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String VOC_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName"
                + " from VOC_Term_View tv, MGI_Synonym s"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1"
                + " and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " and s._MGIType_key = 13 ";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_syn = execute(VOC_SYN_KEY);
        rs_syn.next();

        writeEnd = new Date();

        log.info("Time taken gather vocab synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_syn.isAfterLast()) {

            nonIDToken.setData(rs_syn.getString("synonym"));
            sis.push(nonIDToken.getDocument());
            nonIDToken.clear();
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
    
    private void doVocabNotes() throws SQLException, InterruptedException {

        // SQL for this subsection, please note that the order by clause is
        // important
        // for this sql statement.

        String VOC_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " order by tv._Term_key, t.sequenceNum";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_note = execute(VOC_NOTE_KEY);
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
                    sis.push(nonIDToken.getDocument());
                    nonIDToken.clear();
                }
                place = rs_note.getInt("_Term_key");
            }
            nonIDToken.appendData(rs_note.getString("note"));
            rs_note.next();
        }

        log.info("Done Vocab Notes/Definitions!");
    }

    /**
     * Gather the AD Vocab Terms.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabADTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection

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

            nonIDToken.setData("TS" + rs_ad_term.getString("_Stage_key")
                    + " "
                    + rs_ad_term.getString("printName").replaceAll(";", " "));

            sis.push(nonIDToken.getDocument());
            // Transformed version, w/o TS
            nonIDToken.setData(rs_ad_term.getString("printName").replaceAll(
                    ";", " "));
            sis.push(nonIDToken.getDocument());

            nonIDToken.clear();
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

            nonIDToken.setData(rs_ad_syn.getString("Structure"));
            sis.push(nonIDToken.getDocument());
            nonIDToken.clear();
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
                
                nonIDToken.setData(rs.getString("label"));
        
                sis.push(nonIDToken.getDocument());
                nonIDToken.clear();
                rs.next();
            }
        
            // Clean up
        
            rs.close();
            log.info("Done Marker Labels!");
        
        }
}

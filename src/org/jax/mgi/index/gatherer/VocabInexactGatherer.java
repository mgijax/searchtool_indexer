package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.VocabInexactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * The vocab inexact gatherer is responsible for gathering the small token 
 * relevant information from our vocabulary data sources. 
 *  
 * @author mhall
 *
 * @has A single reference to a VocabDisplayLucene Doc Builder, which is used
 *      to create Lucene documents to place onto the stack.
 *      
 *      A hash map used to convert database codes to human readable display 
 *      values.
 * 
 * @does Upon being started this runs through a group of methods, each of 
 * which are responsible for gathering documents from a different accession id
 * type.
 * 
 * Each subprocess basically operates as follows:
 * 
 * Gather the data for the specific sub type, parse it while creating Lucene 
 * documents and adding them to the stack.  
 * 
 * After it completes parsing, it cleans up its result sets, and exits.
 * 
 * After all of these methods complete, we set gathering complete to true in 
 * the shared document stack and exit.
 */

public class VocabInexactGatherer extends AbstractGatherer {

    // Class Variables

    private Date writeStart;
    private Date writeEnd;

    private VocabInexactLuceneDocBuilder vildb = 
        new VocabInexactLuceneDocBuilder();

    private Logger log = 
        Logger.getLogger(VocabInexactGatherer.class.getName());
    
    // SQL Section

    HashMap<String, String> hm = new HashMap<String, String>();

    /**
     * Get a new copy of the Vocab Inexact gatherer, and set up its hashmap.
     * 
     * @param config
     */

    public VocabInexactGatherer(IndexCfg config) {
        super(config);

        hm.put(IndexConstants.VOCAB_TERM, "Term");
        hm.put(IndexConstants.VOCAB_SYNONYM, "Synonym");
        hm.put(IndexConstants.VOCAB_NOTE, "Definition");
    }

    /**
     * Encapsulates the algorithm used to gather up all the information needed
     * for the Vocab Inexact index.
     */

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
     * Gather the Vocab Non AD Term data.
     * 
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

        ResultSet rs = execute(VOC_TERM_KEY);
        rs.next();

        writeEnd = new Date();
        log.info("Time taken gather non ad terms result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs.isAfterLast()) {

            vildb.setData(rs.getString("term"));
            vildb.setRaw_data(rs.getString("term"));
            vildb.setDb_key(rs.getString("_Term_key"));
            vildb.setVocabulary(rs.getString("vocabName"));
            vildb.setDataType(IndexConstants.VOCAB_TERM);
            vildb.setDisplay_type(hm.get(IndexConstants.VOCAB_TERM));
            
            // Place the document on the stack
            
            sis.push(vildb.getDocument());
            vildb.clear();
            rs.next();
        }

        rs.close();
        log.info("Done gathering Vocab Non AD Terms!");

    }

    /**
     * Gather the Vocab non AD Synonym data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    private void doVocabSynonym() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String VOC_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName"
                + " from VOC_Term_View tv, MGI_Synonym s"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " and s._MGIType_key = 13 ";

        // gather the data

        writeStart = new Date();

        ResultSet rs_syn = execute(VOC_SYN_KEY);
        rs_syn.next();

        writeEnd = new Date();
        log.info("Time taken gather non ad synonyms result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // parse it

        while (!rs_syn.isAfterLast()) {

            vildb.setData(rs_syn.getString("synonym"));
            vildb.setRaw_data(rs_syn.getString("synonym"));
            vildb.setDb_key(rs_syn.getString("_Term_key"));
            vildb.setVocabulary(rs_syn.getString("vocabName"));
            vildb.setDataType(IndexConstants.VOCAB_SYNONYM);
            vildb.setDisplay_type(hm.get(IndexConstants.VOCAB_SYNONYM));
            
            // Place the document on the stack
            
            sis.push(vildb.getDocument());
            vildb.clear();
            rs_syn.next();
        }

        // Clean up

        rs_syn.close();
        log.info("Done gathering Vocab Non AD Synonyms!");
    }

    /**
     * Gather the non AD note/Definition information. Please note that this 
     * is a compound data row as such its parsing code is a bit unique.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doVocabNote() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String VOC_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " order by tv._Term_key, t.sequenceNum";

        // Gather the data

        // Since notes are compound rows in the database, we have to
        // construct the searchable field.

        writeStart = new Date();

        ResultSet rs_note = execute(VOC_NOTE_KEY);
        rs_note.next();

        writeEnd = new Date();
        log.info("Time taken gather non ad notes/definitions result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        int place = -1;

        while (!rs_note.isAfterLast()) {
            if (place != rs_note.getInt("_Term_key")) {
                if (place != -1) {
                    vildb.setRaw_data(vildb.getData());
                    
                    // Place the document on the stack
                    
                    sis.push(vildb.getDocument());
                    vildb.clear();
                }
                vildb.setDb_key(rs_note.getString("_Term_key"));
                vildb.setVocabulary(rs_note.getString("vocabName"));
                vildb.setDataType(IndexConstants.VOCAB_NOTE);
                vildb.setDisplay_type(hm.get(IndexConstants.VOCAB_NOTE));
                place = rs_note.getInt("_Term_key");
            }
            vildb.appendData(rs_note.getString("note"));
            rs_note.next();
        }

        sis.push(vildb.getDocument());

        // Clean up

        rs_note.close();
        log.info("Done gathering Vocab Non AD Notes!");

    }

    /**
     * Gather the Vocab AD Term data.
     * 
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
        log.info("Time taken gather ad terms result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_term.isAfterLast()) {
            // Add in the TS form
            vildb.setData("TS" + rs_ad_term.getString("_Stage_key") + " "
                    + rs_ad_term.getString("printName").replaceAll(";", " "));
            vildb.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            vildb.setDb_key(rs_ad_term.getString("_Structure_key"));
            vildb.setVocabulary(rs_ad_term.getString("vocabName"));
            vildb.setDataType(IndexConstants.VOCAB_TERM);
            vildb.setDisplay_type(hm.get(IndexConstants.VOCAB_TERM));
            
            // Place the document on the stack
            
            sis.push(vildb.getDocument());
            // Transformed version, w/o TS
            vildb.setData(rs_ad_term.getString("printName").replaceAll(";", " "));
            
            // Place the document on the stack
            
            sis.push(vildb.getDocument());

            vildb.clear();
            rs_ad_term.next();
        }

        // Clean up

        rs_ad_term.close();
        log.info("Done gathering Vocab AD Terms!");

    }

    /**
     * Gather the Vocab AD Synonym data.
     * 
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
        log.info("Time taken gather ad synonym result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // parse it

        while (!rs_ad_syn.isAfterLast()) {

            vildb.setData(rs_ad_syn.getString("Structure"));
            vildb.setRaw_data(rs_ad_syn.getString("Structure"));
            vildb.setDb_key(rs_ad_syn.getString("_Structure_key"));
            vildb.setVocabulary(rs_ad_syn.getString("vocabName"));
            vildb.setDataType(IndexConstants.VOCAB_SYNONYM);
            vildb.setDisplay_type(hm.get(IndexConstants.VOCAB_SYNONYM));
            
            // Place the document on the stack
            
            sis.push(vildb.getDocument());
            vildb.clear();
            rs_ad_syn.next();
        }

        // Clean up

        rs_ad_syn.close();
        log.info("Done gathering Vocab AD Synonyms!");
    }
}

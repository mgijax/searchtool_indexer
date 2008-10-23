package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.VocabExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;

import QS_Commons.IndexConstants;

public class VocabExactGatherer extends AbstractGatherer {

    // Class Variables

    private Date                       writeStart;
    private Date                       writeEnd;

    private VocabExactLuceneDocBuilder new_vocab = new VocabExactLuceneDocBuilder();

    private HashMap<String, String>    hm        = new HashMap<String, String>();

    private Logger log = Logger.getLogger(VocabExactGatherer.class.getName());
    
    public VocabExactGatherer(IndexCfg config) {
        super(config);

        hm.put(IndexConstants.VOCAB_TERM, "Term");
        hm.put(IndexConstants.VOCAB_SYNONYM, "Synonym");
        hm.put(IndexConstants.VOCAB_NOTE, "Definition");
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

        String VOC_TERM_KEY = "select _Term_key, term, vocabName"
                + " from VOC_Term_View"
                + " where isObsolete != 1 and _Vocab_key in (44, 4, 5, 8, 46)";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_non_ad_term = execute(VOC_TERM_KEY);
        rs_non_ad_term.next();

        writeEnd = new Date();

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_non_ad_term.isAfterLast()) {

            new_vocab.setVocabulary(rs_non_ad_term.getString("vocabName"));
            new_vocab.setData(rs_non_ad_term.getString("term"));
            new_vocab.setRaw_data(rs_non_ad_term.getString("term"));
            new_vocab.setDb_key(rs_non_ad_term.getString("_Term_key"));
            new_vocab.setDataType(IndexConstants.VOCAB_TERM);
            new_vocab.setDisplay_type(hm.get(IndexConstants.VOCAB_TERM));
            sis.push(new_vocab.getDocument());
            new_vocab.clear();
            rs_non_ad_term.next();
        }

        // Clean up

        rs_non_ad_term.close();
        log.info("Done collecting Vocab Non AD Terms!");

    }

    /**
     * Gather the non ad vocab synonyms.
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

        // Gather the Data

        writeStart = new Date();

        ResultSet rs_non_ad_syn = execute(VOC_SYN_KEY);
        rs_non_ad_syn.next();

        writeEnd = new Date();

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_non_ad_syn.isAfterLast()) {
            new_vocab.setData(rs_non_ad_syn.getString("synonym"));
            new_vocab.setRaw_data(rs_non_ad_syn.getString("synonym"));
            new_vocab.setDb_key(rs_non_ad_syn.getString("_Term_key"));
            new_vocab.setVocabulary(rs_non_ad_syn.getString("vocabName"));
            new_vocab.setDataType(IndexConstants.VOCAB_SYNONYM);
            new_vocab.setDisplay_type(hm.get(IndexConstants.VOCAB_SYNONYM));
            sis.push(new_vocab.getDocument());
            new_vocab.clear();
            rs_non_ad_syn.next();
        }

        // Clean up

        rs_non_ad_syn.close();
        log.info("Done collecting Vocab Non AD Synonyms!");

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

        String VOC_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName"
                + " from VOC_Term_View tv, VOC_text t"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " order by tv._Term_key, t.sequenceNum";

        // Gather the data.

        writeStart = new Date();

        ResultSet rs_non_ad_note = execute(VOC_NOTE_KEY);
        rs_non_ad_note.next();

        writeEnd = new Date();

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        int place = -1;

        while (!rs_non_ad_note.isAfterLast()) {
            if (place != rs_non_ad_note.getInt("_Term_key")) {
                if (place != -1) {
                    new_vocab.setRaw_data(new_vocab.getData());
                    sis.push(new_vocab.getDocument());
                    new_vocab.clear();
                }

                new_vocab.setDb_key(rs_non_ad_note.getString("_Term_key"));
                new_vocab.setVocabulary(rs_non_ad_note.getString("vocabName"));
                new_vocab.setDataType(IndexConstants.VOCAB_NOTE);
                new_vocab.setDisplay_type(hm.get(IndexConstants.VOCAB_NOTE));
                place = rs_non_ad_note.getInt("_Term_key");
            }
            new_vocab.appendData(rs_non_ad_note.getString("note"));
            rs_non_ad_note.next();
        }

        // Clean up

        rs_non_ad_note.close();
        log.info("Done collecting Vocab Non AD Notes/Definitions");

    }

    /**
     * Gather the AD Vocab Terms
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

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_term.isAfterLast()) {
            // Add in the TS form w/o space
            new_vocab.setData("TS" + rs_ad_term.getString("_Stage_key") + ":"
                    + rs_ad_term.getString("printName"));
            new_vocab.setRaw_data("TS" + rs_ad_term.getString("_Stage_key")
                    + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            new_vocab.setDb_key(rs_ad_term.getString("_Structure_key"));
            new_vocab.setVocabulary(rs_ad_term.getString("vocabName"));
            new_vocab.setDataType(IndexConstants.VOCAB_TERM);
            new_vocab.setDisplay_type(hm.get(IndexConstants.VOCAB_TERM));
            sis.push(new_vocab.getDocument());
            // Untransformed version, w/ space
            new_vocab.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName"));
            sis.push(new_vocab.getDocument());
            // Untransformed version, w/o TS
            new_vocab.setData(rs_ad_term.getString("printName"));
            sis.push(new_vocab.getDocument());
            // Transformed version, w/ TS
            new_vocab.setData("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            sis.push(new_vocab.getDocument());

            new_vocab.clear();
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

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ad_syn.isAfterLast()) {
            new_vocab.setData(rs_ad_syn.getString("Structure"));
            new_vocab.setRaw_data(rs_ad_syn.getString("Structure"));
            new_vocab.setDb_key(rs_ad_syn.getString("_Structure_key"));
            new_vocab.setVocabulary(rs_ad_syn.getString("vocabName"));
            new_vocab.setDataType(IndexConstants.VOCAB_SYNONYM);
            new_vocab.setDisplay_type(hm.get(IndexConstants.VOCAB_SYNONYM));
            sis.push(new_vocab.getDocument());
            new_vocab.clear();
            rs_ad_syn.next();
        }

        // Clean up
        
        rs_ad_syn.close();
        log.info("Done collecting Vocab AD Synonyms!");

    }
}

package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.VocabInexactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * The vocab inexact gatherer is responsible for gathering the small token 
 * relevant information from our vocabulary data sources. 
 * 
 * This information is then used to populate the vocabInexact index.
 *  
 * @author mhall
 *
 * @has An instance of the IndexCfg object, which is used to setup this object.
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

public class VocabInexactGatherer extends DatabaseGatherer {

    // Class Variables

    private VocabInexactLuceneDocBuilder builder = 
        new VocabInexactLuceneDocBuilder();

    HashMap<String, String> providerMap = new HashMap<String, String>();

    /**
     * Get a new copy of the Vocab Inexact gatherer, and set up its hashmap.
     * 
     * @param config
     */

    public VocabInexactGatherer(IndexCfg config) {
        super(config);

        providerMap.put(IndexConstants.VOCAB_TERM, "Term");
        providerMap.put(IndexConstants.VOCAB_SYNONYM, "Synonym");
        providerMap.put(IndexConstants.VOCAB_NOTE, "Definition");
    }

    /**
     * Encapsulates the algorithm used to gather up all the information needed
     * for the Vocab Inexact index.
     */

    public void runLocal() throws Exception {
            doVocabTerm();
            doVocabSynonym();
            doVocabNote();
            doVocabADTerm();
            doVocabADSynonym();
    }

    /**
     * Gather the Vocab Non AD Term data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    private void doVocabTerm() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // gather up vocab term, that are not obsolete for all vocabs but ad.
        // Currently this list includes: GO, MP, OMIM, InterPro, and PIRSF
        
        String VOC_TERM_KEY = "select _Term_key, term, vocabName"
                + " from VOC_Term_View"
                + " where isObsolete != 1 and _Vocab_key in (44, 4, 5, 8, 46)";

        // Gather the data

        ResultSet rs = executor.executeMGD(VOC_TERM_KEY);
        rs.next();
        log.info("Time taken gather non ad terms result set: "
                + executor.getTiming());

        // Parse it

        while (!rs.isAfterLast()) {

            builder.setData(rs.getString("term"));
            builder.setRaw_data(rs.getString("term"));
            builder.setDb_key(rs.getString("_Term_key"));
            builder.setVocabulary(rs.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_TERM);
            builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_TERM));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();
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

        // Gather up vocab synonyms for all vocabularies with the exception of 
        // AD.
        
        // Currently this list includes: GO, MP, OMIM, Interpro and PIRSF
        
        String VOC_SYN_KEY = "select tv._Term_key, s.synonym, tv.vocabName"
                + " from VOC_Term_View tv, MGI_Synonym s"
                + " where tv._Term_key = s._Object_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " and s._MGIType_key = 13 ";

        // gather the data

        ResultSet rs_syn = executor.executeMGD(VOC_SYN_KEY);
        rs_syn.next();
        log.info("Time taken gather non ad synonyms result set: "
                + executor.getTiming());

        // parse it

        while (!rs_syn.isAfterLast()) {

            builder.setData(rs_syn.getString("synonym"));
            builder.setRaw_data(rs_syn.getString("synonym"));
            builder.setDb_key(rs_syn.getString("_Term_key"));
            builder.setVocabulary(rs_syn.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_SYNONYM);
            builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_SYNONYM));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();
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

        // Gather up the vocabulary notes for all vocabs except for AD.
        // Currently this list includes: GO, MP, OMIM, PIRSH and Interpro.
        // This list is sorted in sequence order, so the notes can be 
        // reconstructed in the lucene document
        
        String VOC_NOTE_KEY = "select tv._Term_key, t.note, tv.vocabName, t.sequenceNum "
                + " from VOC_Term_View tv, VOC_text t"
                + " where tv._Term_key = t._Term_key and tv.isObsolete != 1 "
                + "and tv._Vocab_key in (44, 4, 5, 8, 46)"
                + " order by tv._Term_key, t.sequenceNum";

        // Gather the data

        // Since notes are compound rows in the database, we have to
        // construct the searchable field.

        ResultSet rs_note = executor.executeMGD(VOC_NOTE_KEY);
        rs_note.next();
        log.info("Time taken gather non ad notes/definitions result set: "
                + executor.getTiming());

        // Parse it

        int place = -1;

        while (!rs_note.isAfterLast()) {
            if (place != rs_note.getInt("_Term_key")) {
                if (place != -1) {
                    builder.setRaw_data(builder.getData());
                    
                    // Place the document on the stack
                    
                    documentStore.push(builder.getDocument());
                    builder.clear();
                }
                builder.setDb_key(rs_note.getString("_Term_key"));
                builder.setVocabulary(rs_note.getString("vocabName"));
                builder.setDataType(IndexConstants.VOCAB_NOTE);
                builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_NOTE));
                place = rs_note.getInt("_Term_key");
            }
            builder.appendData(rs_note.getString("note"));
            rs_note.next();
        }

        documentStore.push(builder.getDocument());

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

        // Gather up the ad terms, excluding top level terms.
        
        String VOC_AD_TERM_KEY = "select s._Structure_key, s._Stage_key, "
                + "s.printName, 'AD' as vocabName" + " from GXD_Structure s"
                + " where s._Parent_key is not null";

        // Gather the data

        ResultSet rs_ad_term = executor.executeMGD(VOC_AD_TERM_KEY);
        rs_ad_term.next();
        log.info("Time taken gather ad terms result set: "
                + executor.getTiming());

        // Parse it

        while (!rs_ad_term.isAfterLast()) {
            // Add in the TS form
            builder.setData("TS" + rs_ad_term.getString("_Stage_key") + " "
                    + rs_ad_term.getString("printName").replaceAll(";", " "));
            builder.setRaw_data("TS" + rs_ad_term.getString("_Stage_key") + ": "
                    + rs_ad_term.getString("printName").replaceAll(";", "; "));
            builder.setDb_key(rs_ad_term.getString("_Structure_key"));
            builder.setVocabulary(rs_ad_term.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_TERM);
            builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_TERM));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            // Transformed version, w/o TS
            builder.setData(rs_ad_term.getString("printName")
                    .replaceAll(";", " "));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());

            builder.clear();
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

        // Gather up ad synonyms, excluding top level terms.
        
        String VOC_AD_SYN_KEY = "select s._Structure_key, sn.Structure, "
                + "'AD' as vocabName"
                + " from GXD_Structure s, GXD_StructureName sn"
                + " where s._parent_key is not null"
                + " and s._Structure_key = sn._Structure_key and "
                + "s._StructureName_key != sn._StructureName_key";

        // Gather the data

        ResultSet rs_ad_syn = executor.executeMGD(VOC_AD_SYN_KEY);
        rs_ad_syn.next();
        log.info("Time taken gather ad synonym result set: "
                + executor.getTiming());

        // parse it

        while (!rs_ad_syn.isAfterLast()) {

            builder.setData(rs_ad_syn.getString("Structure"));
            builder.setRaw_data(rs_ad_syn.getString("Structure"));
            builder.setDb_key(rs_ad_syn.getString("_Structure_key"));
            builder.setVocabulary(rs_ad_syn.getString("vocabName"));
            builder.setDataType(IndexConstants.VOCAB_SYNONYM);
            builder.setDisplay_type(providerMap.get(IndexConstants.VOCAB_SYNONYM));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_ad_syn.next();
        }

        // Clean up

        rs_ad_syn.close();
        log.info("Done gathering Vocab AD Synonyms!");
    }
}

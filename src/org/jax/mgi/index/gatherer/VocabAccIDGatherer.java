package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.VocabExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Gather up all of the information needed to search for vocabulary terms by 
 * accession id.
 *  
 * @author mhall
 *
 * @does Upon being started this runs through a group of methods, each of 
 * which are responsible for gathering documents from a different accession id
 * type.
 * 
 * Each subprocess basically operates as follows:
 * 
 * Gather the data for the specific sub-type, parse it while creating lucene 
 * documents and adding them to the stack.  
 * 
 * After it completes parsing, it cleans up its result sets, and exits.
 * 
 * After all of these methods complete, we set gathering complete to true in 
 * the shared document stack and exit.
 */

public class VocabAccIDGatherer extends AbstractGatherer {

    // Class Variables

    private Date writeStart;
    private Date writeEnd;

    private VocabExactLuceneDocBuilder veldb = 
        new VocabExactLuceneDocBuilder();

    private HashMap<String, String> hm = new HashMap<String, String>();

    private Logger log = Logger.getLogger(VocabAccIDGatherer.class.getName());
    
    public VocabAccIDGatherer(IndexCfg config) {
        super(config);

        hm.put(IndexConstants.VOCAB_TERM, "Term");
        hm.put(IndexConstants.VOCAB_SYNONYM, "Term Synonym");
        hm.put(IndexConstants.VOCAB_NOTE, "Term Definition");
        hm.put(IndexConstants.ACCESSION_ID, "ID");
    }

    public void run() {
        try {

            doVocabAccessionID();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Gather the vocabulary accession ID's.  Please note that AD has no 
     * accession ID's
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabAccessionID() 
        throws SQLException, InterruptedException {
        
        //SQL for this Subsection
        
        String VOC_ACCID_KEY = "select _Term_key, accId, vocabName "
                + " from VOC_Term_View"
                + " where isObsolete != 1 and _Vocab_key in (44, 4, 5, 8, 46)";

        // Gather the data
        
        writeStart = new Date();

        ResultSet rs_acc_id = execute(VOC_ACCID_KEY);
        rs_acc_id.next();

        writeEnd = new Date();

        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        while (!rs_acc_id.isAfterLast()) {
            veldb.setData(rs_acc_id.getString("accId"));
            veldb.setRaw_data(rs_acc_id.getString("accId"));
            veldb.setDb_key(rs_acc_id.getString("_Term_key"));
            veldb.setVocabulary(rs_acc_id.getString("vocabName"));
            
            // Omim terms have a provider which must be displayed.
            
            if (rs_acc_id.getString("vocabName").equals(
                    IndexConstants.OMIM_TYPE_NAME)) {
                veldb.setProvider("(OMIM)");
            }
            veldb.setDataType(IndexConstants.ACCESSION_ID);
            veldb.setDisplay_type(hm.get(IndexConstants.ACCESSION_ID));
            
            // Place the document on the stack.
            
            sis.push(veldb.getDocument());
            veldb.clear();
            rs_acc_id.next();
        }

        // Clean up
        
        rs_acc_id.close();
        log.info("Done collecting Vocab Accession IDs!");

    }
}

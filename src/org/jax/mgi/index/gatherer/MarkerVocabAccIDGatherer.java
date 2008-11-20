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
 * Gather up all of the information needed to search for markers, by 
 * vocabulary accession ID.
 *  
 * @author mhall
 *
 */

public class MarkerVocabAccIDGatherer extends AbstractGatherer {

    // Class Variables

    private Date                       writeStart;
    private Date                       writeEnd;

    private VocabExactLuceneDocBuilder new_vocab =
        new VocabExactLuceneDocBuilder();

    private Logger log = Logger.getLogger(
            MarkerVocabAccIDGatherer.class.getName());
    
    private HashMap<String, String>    hm =
        new HashMap<String, String>();

    public MarkerVocabAccIDGatherer(IndexCfg config) {
        super(config);

        hm.put(IndexConstants.ACCESSION_ID, "ID");
        
        /* 
         * Vocab Providers are special, so we will use the HashMap to take
         * take care of this relationship.
         */
        
        hm.put("Mammalian Phenotype", "Phenotype");
        hm.put("PIR Superfamily", "Protein Family");
        hm.put("InterPro Domains", "Protein Domain");
        hm.put(IndexConstants.OMIM_TYPE_NAME, "Disease Model");
        hm.put(IndexConstants.OMIM_ORTH_TYPE_NAME, "Disease Ortholog");
        hm.put(IndexConstants.GO_TYPE_NAME, "Function");
        
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
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabAccessionID() 
        throws SQLException, InterruptedException {
        
        //SQL for this Subsection
        // As long as the column names remain the same, the SQL for any 
        // given vocabulary can move independently of each other.

        log.info("Collecting GO Accession ID's");
        
        String GO_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
                + " tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 4"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'GO/Marker'";

        doVocabAccessionID(GO_ACCID_KEY);

        log.info("Collecting MP Accession ID's");
        
        String MP_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
                + " tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 5"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'Mammalian Phenotype/Genotype'";

        doVocabAccessionID(MP_ACCID_KEY);

        log.info("Collecting Interpro Accession ID's");
        
        String INTERPRO_ACCID_KEY = "select tv._Term_key, tv.accId,"
                + " tv.vocabName, tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 8"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'InterPro/Marker'";

        doVocabAccessionID(INTERPRO_ACCID_KEY);

        log.info("Collecting PIRSF Accession ID's");
        
        String PIRSF_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
                + " tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 46"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'PIRSF/Marker'";

        doVocabAccessionID(PIRSF_ACCID_KEY);

        log.info("Collecting OMIM Accession ID's");
        
        String OMIM_ACCID_KEY = "select tv._Term_key, tv.accId, tv.vocabName,"
                + " tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Genotype'";

        doVocabAccessionID(OMIM_ACCID_KEY);

        log.info("Collecting OMIM/Human Accession ID's");
        
        String OMIM_HUMAN_ACCID_KEY = "select tv._Term_key, tv.accId," + " '"
                + IndexConstants.OMIM_ORTH_TYPE_NAME + "' as vocabName,"
                + " tv.term"
                + " from VOC_Term_View tv, VOC_Annot_Count_Cache vacc"
                + " where isObsolete != 1 and _Vocab_key = 44"
                + " and tv._Term_key = vacc._Term_key and vacc.annotType ="
                + " 'OMIM/Human Marker'";

        doVocabAccessionID(OMIM_HUMAN_ACCID_KEY);
  
        log.info("Done collecting All Vocab Accession IDs!");

    }

    /**
     * Generic Accession ID Gathering Algorithm.  It takes a SQL string 
     * with an assumed format, and creates Lucene Documents from it.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doVocabAccessionID(String sql) 
        throws SQLException, InterruptedException {
        
        // Gather the data
        
        writeStart = new Date();
    
        ResultSet rs_acc_id = execute(sql);
        rs_acc_id.next();
    
        writeEnd = new Date();
    
        log.info("Time taken gather result set: "
                + (writeEnd.getTime() - writeStart.getTime()));
    
        // Parse it
        
        while (!rs_acc_id.isAfterLast()) {
            new_vocab.setData(rs_acc_id.getString("accId"));
            new_vocab.setRaw_data(rs_acc_id.getString("term"));
            new_vocab.setDb_key(rs_acc_id.getString("_Term_key"));
            new_vocab.setVocabulary(rs_acc_id.getString("vocabName"));
            new_vocab.setDataType(IndexConstants.VOC_ACCESSION_ID); 
            new_vocab.setDisplay_type(hm.get(
                    rs_acc_id.getString("vocabName")));
            new_vocab.setProvider("("+rs_acc_id.getString("accId")+")");
            sis.push(new_vocab.getDocument());
            new_vocab.clear();
            rs_acc_id.next();
        }
    
        // Clean up
        
        rs_acc_id.close();
    
    }
}

package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.OtherExactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.searchtoolIndexer.util.ProviderHashMap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up all the different sorts of 
 * information that we need to perform searches against accession id's.  It is
 * however important to note that this is not always direct accid->object 
 * relations.  We do more complex things like es cell line 
 * accession id's -> alleles, and probes -> sequences as well.
 * 
 * This information is then used to populate the otherExact index.
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
 * Gather the data for the specific subtype, parse it while creating lucene 
 * documents and adding them to the stack.  
 * 
 * After it completes parsing, it cleans up its result sets, and exits.
 * 
 * After all of these methods complete, we set gathering complete to true in 
 * the shared document stack and exit.
 *
 */

public class OtherExactGatherer extends DatabaseGatherer {

    // Class Variables

    private double total = 0;
    private double output_incrementer = 100000;
    private double output_threshold = 100000;

    // The single LuceneDocBuilder for this Object.

    private OtherExactLuceneDocBuilder builder =
        new OtherExactLuceneDocBuilder();

    private ProviderHashMap phm;

    public OtherExactGatherer(IndexCfg config) {
        super(config);
        phm = new ProviderHashMap(config);
    }
    
    /**
     * This is the runLocal method, which is called by the supers run() method.
     * It encapsulates the work that this specific implementing object needs to
     * perform in order to get its work done.
     */

    public void runLocal() throws Exception {
            doOrthologs();
            doReferences();
            doProbes();
            doAssays();
            doAntibodies();
            doAntigens();
            doExperiments();
            doImages();
            doSequences();
            doSequencesByProbe();
            doSnps();
            doSubSnps();
            doAMA(); 
    }

    /**
     * Gather reference data.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doReferences() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // Gather up the non private accession ids for references.
        
        String OTHER_REF_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, '" + IndexConstants.OTHER_REFERENCE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 1";

        // Gather the data

        ResultSet rs_ref = executor.executeMGD(OTHER_REF_SEARCH);
        rs_ref.next();

        log.info("Time taken gather reference data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_ref.isAfterLast()) {

            builder.setType(rs_ref.getString("_MGIType_key"));
            builder.setData(rs_ref.getString("accID"));
            builder.setDb_key(rs_ref.getString("_Object_key"));
            builder.setAccessionKey(rs_ref.getString("_Accession_key"));
            builder.setPreferred(rs_ref.getString("preferred"));
            builder.setProvider(phm.get(rs_ref.getString("_LogicalDB_key")));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_ref.next();
        }

        // Clean up

        log.info("Done creating documents for references!");
        rs_ref.close();
    }

    /**
     * Gather the probe data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doProbes() throws SQLException, InterruptedException {

        // SQL For this Subsection

        // gather up the non private accession id's for probes.
        
        String OTHER_PROBE_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, '" + IndexConstants.OTHER_PROBE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 3";

        // Gather the data.

        ResultSet rs_prb = executor.executeMGD(OTHER_PROBE_SEARCH);
        rs_prb.next();

        log.info("Time taken gather probe data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_prb.isAfterLast()) {

            builder.setType(rs_prb.getString("_MGIType_key"));
            builder.setData(rs_prb.getString("accID"));
            builder.setDb_key(rs_prb.getString("_Object_key"));
            builder.setAccessionKey(rs_prb.getString("_Accession_key"));
            builder.setPreferred(rs_prb.getString("preferred"));
            builder.setProvider(phm.get(rs_prb.getString("_LogicalDB_key")));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_prb.next();
        }

        // Clean up

        log.info("Done creating documents for probes!");
        rs_prb.close();
    }

    /**
     * Gather the assay data.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doAssays() throws SQLException, InterruptedException {

        // SQL for this Subsection.

        // gather up the non private accession ids for assays
        
        String OTHER_ASSAY_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ASSAY' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 8";

        // Gather the data

        ResultSet rs_assay = executor.executeMGD(OTHER_ASSAY_SEARCH);
        rs_assay.next();

        log.info("Time taken gather assay data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_assay.isAfterLast()) {

            builder.setType(rs_assay.getString("_MGIType_key"));
            builder.setData(rs_assay.getString("accID"));
            builder.setDb_key(rs_assay.getString("_Object_key"));
            builder.setAccessionKey(rs_assay.getString("_Accession_key"));
            builder.setPreferred(rs_assay.getString("preferred"));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_assay.next();
        }

        // Clean up

        log.info("Done creating documents for assays!");
        rs_assay.close();

    }

    /**
     * Gather the antibody data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAntibodies() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // gather up the non private accession id's for anitbodies.
        
        String OTHER_ANTIBODY_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ANTIBODY' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 6";

        // Gather the data

        ResultSet rs_anti = executor.executeMGD(OTHER_ANTIBODY_SEARCH);
        rs_anti.next();

        log.info("Time taken gather antibody data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_anti.isAfterLast()) {

            builder.setType(rs_anti.getString("_MGIType_key"));
            builder.setData(rs_anti.getString("accID"));
            builder.setDb_key(rs_anti.getString("_Object_key"));
            builder.setAccessionKey(rs_anti.getString("_Accession_key"));
            builder.setPreferred(rs_anti.getString("preferred"));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the documents on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_anti.next();
        }

        // Clean up

        log.info("Done creating documents for antibodies!");
        rs_anti.close();
    }

    /**
     * Gather the antigen data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAntigens() throws SQLException, InterruptedException {

        // SQL for this Subsection.

        // gather up the non private accession id's for anitgens.
        
        String OTHER_ANTIGEN_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ANTIGEN' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 7";

        // Gather the data

        ResultSet rs_antigen = executor.executeMGD(OTHER_ANTIGEN_SEARCH);
        rs_antigen.next();

        log.info("Time taken gather antigen data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_antigen.isAfterLast()) {

            builder.setType(rs_antigen.getString("_MGIType_key"));
            builder.setData(rs_antigen.getString("accID"));
            builder.setDb_key(rs_antigen.getString("_Object_key"));
            builder.setAccessionKey(rs_antigen.getString("_Accession_key"));
            builder.setPreferred(rs_antigen.getString("preferred"));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_antigen.next();
        }

        // Clean up

        log.info("Done creating documents for antigens!");
        rs_antigen.close();

    }

    /**
     * Gather the experiment data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doExperiments() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // Gather up the non private accession id's for experiments.
        
        String OTHER_EXPERIMENT_SEARCH = "SELECT distinct a._Accession_key, "
                + " a.accID, a._Object_key, 'EXPERIMENT' as _MGIType_key,"
                + " a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 4";

        // Gather the data

        ResultSet rs_exp = executor.executeMGD(OTHER_EXPERIMENT_SEARCH);
        rs_exp.next();

        log.info("Time taken gather experiment data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_exp.isAfterLast()) {

            builder.setType(rs_exp.getString("_MGIType_key"));
            builder.setData(rs_exp.getString("accID"));
            builder.setDb_key(rs_exp.getString("_Object_key"));
            builder.setAccessionKey(rs_exp.getString("_Accession_key"));
            builder.setPreferred(rs_exp.getString("preferred"));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_exp.next();
        }

        // Clean up

        log.info("Done creating documents for experiments!");
        rs_exp.close();

    }

    /**
     * Gather the image data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doImages() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // Gather up the non private accession id's for images.
        
        String OTHER_IMAGE_SEARCH = "SELECT distinct a._Accession_key,"
                + " a.accID, a._Object_key, 'IMAGE' as _MGIType_key,"
                + " a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key =9";

        // Gather the data.

        ResultSet rs_image = executor.executeMGD(OTHER_IMAGE_SEARCH);
        rs_image.next();

        log.info("Time taken gather image data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_image.isAfterLast()) {

            builder.setType(rs_image.getString("_MGIType_key"));
            builder.setData(rs_image.getString("accID"));
            builder.setDb_key(rs_image.getString("_Object_key"));
            builder.setAccessionKey(rs_image.getString("_Accession_key"));
            builder.setPreferred(rs_image.getString("preferred"));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_image.next();
        }

        // Clean up

        log.info("Done creating documents for images!");
        rs_image.close();
    }

    /**
     * Gather the sequence data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSequences() throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        // gather up the non private accession id's for sequences, for mouse
        // only sequences.

        String OTHER_SEQ_SEARCH = "SELECT distinct a._Accession_key, a.accID,"
                + " a._Object_key, '" + IndexConstants.OTHER_SEQUENCE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a, SEQ_Sequence s"
                + " where a.private != 1 and a._MGIType_key = 19 and"
                + " a._Object_key = s._Sequence_key and"
                + " s._Organism_key = 1";

        // Gather the data.

        ResultSet rs_seq = executor.executeMGD(OTHER_SEQ_SEARCH);
        rs_seq.next();

        log.info("Time taken gather sequence data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_seq.isAfterLast()) {

            builder.setType(rs_seq.getString("_MGIType_key"));
            builder.setData(rs_seq.getString("accID"));
            builder.setDb_key(rs_seq.getString("_Object_key"));
            builder.setAccessionKey(rs_seq.getString("_Accession_key"));
            builder.setPreferred(rs_seq.getString("preferred"));
            builder.setProvider(phm.get(rs_seq.getString("_LogicalDB_key")));

            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }

            rs_seq.next();
        }      
        
        // Clean up

        log.info("Done creating documents for sequences!");
        rs_seq.close();
    }

    /**
     * Gather Sequences by way of Probe Accession ID's. This subsections SQL is
     * significantly more complex than the others.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSequencesByProbe() 
        throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        // Gather up the accession ids for probes, and then assign them to the
        // sequences that these probes point at for non private accession ids
        // where the organism is a mouse.

        String OTHER_SEQ_BY_PROBE_SEARCH = "SELECT distinct a._Accession_key, "
                + " ac.accID, a._Object_key, 'SEQUENCE' as _MGIType_key,"
                + " a.preferred, ac._LogicalDB_key"
                + " FROM ACC_Accession a, SEQ_Sequence s,"
                + " SEQ_Probe_Cache spc, acc_accession ac"
                + " where a.private != 1 and a._MGIType_key = 19 and"
                + " a._Object_key = s._Sequence_key and s._Organism_key = 1"
                + " and a._Object_key = spc._Sequence_key and"
                + " spc._Probe_key = ac._Object_key and ac._MGIType_key = 3"
                + " and AC._LogicalDB_key != 9 and ac.private != 1";

        // Gather the data

        ResultSet rs_seq_by_probe = executor.executeMGD(OTHER_SEQ_BY_PROBE_SEARCH);
        rs_seq_by_probe.next();

        log.info("Time taken gather sequence by probe id data set: "
                + executor.getTiming());

        // Parse it


        while (!rs_seq_by_probe.isAfterLast()) {

            builder.setType(rs_seq_by_probe.getString("_MGIType_key"));
            builder.setData(rs_seq_by_probe.getString("accID"));
            builder.setDb_key(rs_seq_by_probe.getString("_Object_key"));
            builder.setAccessionKey(rs_seq_by_probe.getString("_Accession_key"));
            builder.setPreferred(rs_seq_by_probe.getString("preferred"));
            builder.setProvider(phm.get(rs_seq_by_probe
                    .getString("_LogicalDB_key")));

            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();

            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }

            rs_seq_by_probe.next();
        }

        // Clean up

        log.info("Done creating documents for sequences by probe IDs!");

        rs_seq_by_probe.close();
    }

    /**
     * Gather the ref SNP data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSnps() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // Gather up the snp accession id's
        
        String OTHER_SNP_PRIME_SEARCH = "SELECT distinct _Accession_key,"
                + " accID, _Object_key, 'SNP' as _MGIType_key, 1 as preferred"
                + " FROM SNP_Accession" + " where _MGIType_key = 30";

        // Gather the data

        ResultSet rs_snp_prime = executor.executeSNP(OTHER_SNP_PRIME_SEARCH);
        rs_snp_prime.next();

        log.info("Time taken gather snp data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_snp_prime.isAfterLast()) {

            builder.setType(rs_snp_prime.getString("_MGIType_key"));
            builder.setData(rs_snp_prime.getString("accID"));
            builder.setDb_key(rs_snp_prime.getString("_Object_key"));
            builder.setAccessionKey(rs_snp_prime.getString("_Accession_key"));
            builder.setPreferred(rs_snp_prime.getString("preferred"));
            
            // This is an odd case, as far as I can tell, this is hard coded
            // on the jsp page that this requirement was pulled from.
            
            builder.setProvider("dbSNP");
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_snp_prime.next();
        }

        // Clean up

        log.info("Done creating documents for snps!");
        rs_snp_prime.close();
    }

    /**
     * Gather the Sub SNP data
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSubSnps() throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        // gather up the snp accession ids

        String OTHER_SNP_SECONDARY_SEARCH = "SELECT distinct sa._Accession_key, sa.accID, sss._ConsensusSnp_key " +
        		"as _Object_key, 'SNP' as _MGIType_key, 0 as prefered " +
                "FROM SNP_Accession sa, SNP_SubSnp sss " +
                "where _MGIType_key = 31 and sa._Object_key = sss._SubSnp_key";

        // Gather the data

        ResultSet rs_snp_sub = executor.executeSNP(OTHER_SNP_SECONDARY_SEARCH);
        rs_snp_sub.next();

        log.info("Time taken to gather sub snp data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_snp_sub.isAfterLast()) {

            builder.setType(rs_snp_sub.getString("_MGIType_key"));
            builder.setData(rs_snp_sub.getString("accID"));
            builder.setDb_key(rs_snp_sub.getString("_Object_key"));
            builder.setAccessionKey(rs_snp_sub.getString("_Accession_key"));
            builder.setPreferred(rs_snp_sub.getString("prefered"));
            // This is the same odd case the the other SNP data.
            builder.setProvider("dbSNP");
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_snp_sub.next();
        }

        // Clean up

        log.info("Done creating documents for sub snps!");
        rs_snp_sub.close();
    }

    /**
     * Gather the orthologs data. This has a realized logical db display field.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doOrthologs() throws SQLException, InterruptedException {

        // SQL for this Subsection

	// Get accession IDs for non-mouse markers involved in HomoloGene
	// homology classes.

	String OTHER_ORTHOLOG_SEARCH = "select distinct aa._Accession_key, "
	    + " aa.accID, "
	    + " mm._Marker_key, "
	    + " 'ORTHOLOG' as _MGIType_key, "
	    + " aa.preferred, "
	    + " aa._LogicalDB_key, "
	    + " mo.commonName, "
	    + " hg.accID as HomoloGeneID "
	    + "from VOC_Term source, "
	    + " MRK_Cluster mc, "
	    + " MRK_ClusterMember mcm, "
	    + " MRK_Marker mm, "
	    + " MGI_Organism mo, "
	    + " ACC_Accession aa, "
	    + " ACC_Accession hg "
	    + "where source.term = 'HomoloGene' "
	    + " and source._Term_key = mc._ClusterSource_key "
	    + " and mc._Cluster_key = mcm._Cluster_key "
	    + " and mcm._Marker_key = mm._Marker_key "
	    + " and mm._Organism_key != 1 "
	    + " and mm._Organism_key = mo._Organism_key "
	    + " and mm._Marker_key = aa._Object_key "
	    + " and aa._MGIType_key = 2 "
	    + " and aa.private = 0"
            + " and mc._Cluster_key = hg._Object_key "
	    + " and hg._MGIType_key = 39 " 
	    + " and hg.private = 0"; 

        // gather the data

        ResultSet rs_orthologs = executor.executeMGD(OTHER_ORTHOLOG_SEARCH);
        rs_orthologs.next();

        log.info("Time taken to gather homologous marker id data set: "
                + executor.getTiming());

        // Parse it

	int documentCount = 0;
        while (!rs_orthologs.isAfterLast()) {
	    documentCount++;

            builder.setType(rs_orthologs.getString("_MGIType_key"));
            builder.setData(rs_orthologs.getString("accID"));

            builder.setDb_key(rs_orthologs.getString("_Marker_key"));

            builder.setAccessionKey(rs_orthologs.getString("_Accession_key"));
            builder.setPreferred(rs_orthologs.getString("preferred"));
            
            // This has a realized provider string, we add in the species.
            
            builder.setProvider(phm.get(rs_orthologs.getString("_LogicalDB_key"))
                    + " - " + InitCap.initCap(rs_orthologs.getString("commonName")));

            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();

            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            
            rs_orthologs.next();
        }

        // Clean up

        log.info("Done creating " + documentCount
		+ " documents for homologous marker IDs!");
        rs_orthologs.close();

	doHomoloGeneClasses();
    }

    /**
     * Gather the HomoloGene class data. This has a realized logical db display field.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doHomoloGeneClasses() throws SQLException, InterruptedException {
	// Get accession IDs for the HomoloGene classes themselves.

	String HOMOLOGENE_CLUSTER_SEARCH = 
	    "select distinct aa._Accession_key, "
	    + " aa._Object_key, "
	    + " 'HOMOLOGY' as _MGIType_key, "
	    + " aa.preferred, "
	    + " aa._LogicalDB_key, "
	    + " aa.accID as HomoloGeneID "
	    + "from VOC_Term source, "
	    + " MRK_Cluster mc, "
	    + " ACC_Accession aa "
	    + "where source.term = 'HomoloGene' "
	    + " and source._Term_key = mc._ClusterSource_key "
	    + " and mc._Cluster_key = aa._Object_key "
	    + " and aa._MGIType_key = 39 "
	    + " and aa.private = 0";

        // gather the data

        ResultSet rs_homologene =
	    executor.executeMGD(HOMOLOGENE_CLUSTER_SEARCH);
        rs_homologene.next();

        log.info("Time taken to gather HomoloGene class id data set: "
                + executor.getTiming());

        // Parse it

	int documentCount = 0;
        while (!rs_homologene.isAfterLast()) {
	    documentCount++;

            builder.setType(IndexConstants.OTHER_HOMOLOGY);
            builder.setData(rs_homologene.getString("HomoloGeneID"));

            builder.setDb_key(rs_homologene.getString("HomoloGeneID"));
            builder.setAccessionKey(rs_homologene.getString("_Accession_key"));
            builder.setPreferred(rs_homologene.getString("preferred"));
            builder.setProvider(
		phm.get(rs_homologene.getString("_LogicalDB_key")) );

            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();

            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            
            rs_homologene.next();
        }

        // Clean up

        log.info("Done creating " + documentCount
		+ " documents for HomoloGene class IDs!");
        rs_homologene.close();
    }

    /**
     * Gather Adult Mouse Anatomy data
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doAMA() throws SQLException, InterruptedException {

        // SQL for this Subsection
        
        // Gather up the adult mouse anatomy term accession id's, only for the 
        // preferred accession id's

        String OTHER_AMA_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, 'AMA' as _MGIType_key, "
                + "a.preferred, a._LogicalDB_key"
                + " from acc_accession a, VOC_Term v"
                + " where a.private !=1 and a._MGIType_key = 13 and "
                + "a._Object_key = v._Term_key and v._Vocab_key = 6 "
                + "and a.preferred = 1";

        // Gather the data

        ResultSet rs_ama = executor.executeMGD(OTHER_AMA_SEARCH);
        rs_ama.next();

        log.info("Time taken gather ama data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_ama.isAfterLast()) {

            builder.setType(rs_ama.getString("_MGIType_key"));
            builder.setData(rs_ama.getString("accID"));
            builder.setDb_key(rs_ama.getString("_Object_key"));
            builder.setAccessionKey(rs_ama.getString("_Accession_key"));
            builder.setPreferred(rs_ama.getString("preferred"));
            builder.setProvider(phm.get(rs_ama.getString("_LogicalDB_key")));
            while (documentStore.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            builder.clear();
            rs_ama.next();
        }

        // Clean up

        log.info("Done creating documents for AMA!");
        rs_ama.close();

    }


}

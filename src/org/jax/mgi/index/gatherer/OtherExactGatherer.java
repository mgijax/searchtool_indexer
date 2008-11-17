package org.jax.mgi.index.gatherer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.OtherExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up all the different sorts of information
 * that we need to perform searches against accession id's.  It is however important to 
 * note that this is not always direct accid->object relations.  We do more complex things
 * like escellline accession id's -> alleles, and probes -> sequences as well.
 * 
 * @author mhall
 *
 * @has A single instance of the OtherExactLuceneDocBuilder, which encapsulates the data
 * needed to populate the index.  This object can produce lucene documents on demand.
 * 
 * A single instance of the ProviderHashMap gatherer, which is used to translate logical 
 * db keys into human readable display information.
 * 
 * @does Upon being started this runs through a group of methods, each of which are 
 * responsible for gathering documents from a different accession id type.
 * 
 * Each subprocess basically operates as follows:
 * 
 * Gather the data for the specific subtype, parse it while creating lucene documents and 
 * adding them to the stack.  
 * 
 * After it completes parsing, it cleans up its result sets, and exits.
 * 
 * After all of these methods complete, we set gathering complete to true in the shared
 * document stack and exit.
 *
 */

public class OtherExactGatherer extends AbstractGatherer {

    // Class Variables

    private double                     total              = 0;
    private double                     output_incrementer = 100000;
    private double                     output_threshold   = 100000;

    private Logger log = Logger.getLogger(OtherExactGatherer.class.getName());
    
    private Date                       writeStart;
    private Date                       writeEnd;

    // The single LuceneDocBuilder for this Object.

    private OtherExactLuceneDocBuilder otherExact         = new OtherExactLuceneDocBuilder();

    private ProviderHashMapGatherer    phmg;
    
    // This object needs a special connection.
    
    private Connection                 conSnp;

    public OtherExactGatherer(IndexCfg config) {
        super(config);
        phmg = new ProviderHashMapGatherer(config);
        
        try {
            Class.forName(DB_DRIVER);
            String USER = config.get("MGI_PUBLICUSER");
            String PASSWORD = config.get("MGI_PUBLICPASSWORD");
            stack_max = new Integer(config.get("STACK_MAX"));
            log.debug("SNP_JDBC_URL: " + config.get("SNP_JDBC_URL"));
            conSnp = DriverManager.getConnection(
                    config.get("SNP_JDBC_URL"), USER, PASSWORD);
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * Execute a given SQL Statement.
     * 
     * @param query
     * @return ResultSet
     */

    public ResultSet executeSnp(String query) {
        ResultSet set;

        try {
            java.sql.Statement stmt = conSnp.createStatement();

            set = stmt.executeQuery(query);
            return set;
        } catch (Exception e) {
            log.error(e);
            return null;
        }
    }
    
    public void cleanup() {
        super.cleanup();
        
        try {
            conSnp.close();
        } catch (Exception e) {
            log.error(e);
        }
        
    }
    
    /**
     * This is the encapsulation of the algorithm used to gather all of the
     * information for the OtherExact index. After this has completed gathering
     * its information, it sets the complete flag in the shared document stack,
     * cleans up any resources that it used and exits.
     */

    public void run() {
        try {

            doReferences();

            doProbes();

            doAlleles();

            doAssays();

            doAntibodies();

            doAntigens();

            doExperiments();

            doImages();

            doSequences();

            doSequencesByProbe();

            doSnps();

            doSubSnps();

            doOrthologs();

            doAMA();

            doESCellLines();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Gather reference data.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doReferences() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_REF_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, '" + IndexConstants.OTHER_REFERENCE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 1";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_ref = execute(OTHER_REF_SEARCH);
        rs_ref.next();

        writeEnd = new Date();

        log.info("Time taken gather reference data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ref.isAfterLast()) {

            otherExact.setType(rs_ref.getString("_MGIType_key"));
            otherExact.setData(rs_ref.getString("accID"));
            otherExact.setDb_key(rs_ref.getString("_Object_key"));
            otherExact.setAccessionKey(rs_ref.getString("_Accession_key"));
            otherExact.setPreferred(rs_ref.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_ref
                    .getString("_LogicalDB_key")));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            // log.info(new_other.getData());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_PROBE_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, '" + IndexConstants.OTHER_PROBE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 3";

        // Gather the data.

        writeStart = new Date();

        ResultSet rs_prb = execute(OTHER_PROBE_SEARCH);
        rs_prb.next();

        writeEnd = new Date();

        log.info("Time taken gather probe data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_prb.isAfterLast()) {

            otherExact.setType(rs_prb.getString("_MGIType_key"));
            otherExact.setData(rs_prb.getString("accID"));
            otherExact.setDb_key(rs_prb.getString("_Object_key"));
            otherExact.setAccessionKey(rs_prb.getString("_Accession_key"));
            otherExact.setPreferred(rs_prb.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_prb
                    .getString("_LogicalDB_key")));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
            rs_prb.next();
        }

        // Clean up

        log.info("Done creating documents for probes!");
        rs_prb.close();
    }

    /**
     * Gather the allele data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAlleles() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ALL_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, 'ALLELE' as _MGIType_key, a.preferred, "
                + "a._LogicalDB_key" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 11";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_all = execute(OTHER_ALL_SEARCH);
        rs_all.next();

        writeEnd = new Date();

        log.info("Time taken gather allele data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_all.isAfterLast()) {
            otherExact.setType(rs_all.getString("_MGIType_key"));
            otherExact.setData(rs_all.getString("accID"));
            otherExact.setDb_key(rs_all.getString("_Object_key"));
            otherExact.setAccessionKey(rs_all.getString("_Accession_key"));
            otherExact.setPreferred(rs_all.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_all
                    .getString("_LogicalDB_key")));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
            rs_all.next();
        }

        // Clean up

        log.info("Done creating documents for alleles!");
        rs_all.close();
    }

    /**
     * Gather the assay data.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doAssays() throws SQLException, InterruptedException {

        // SQL for this Subsection.

        String OTHER_ASSAY_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ASSAY' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 8";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_assay = execute(OTHER_ASSAY_SEARCH);
        rs_assay.next();

        writeEnd = new Date();

        log.info("Time taken gather assay data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_assay.isAfterLast()) {

            otherExact.setType(rs_assay.getString("_MGIType_key"));
            otherExact.setData(rs_assay.getString("accID"));
            otherExact.setDb_key(rs_assay.getString("_Object_key"));
            otherExact.setAccessionKey(rs_assay.getString("_Accession_key"));
            otherExact.setPreferred(rs_assay.getString("preferred"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_ANTIBODY_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ANTIBODY' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 6";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_anti = execute(OTHER_ANTIBODY_SEARCH);
        rs_anti.next();

        writeEnd = new Date();

        log.info("Time taken gather antibody data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_anti.isAfterLast()) {

            otherExact.setType(rs_anti.getString("_MGIType_key"));
            otherExact.setData(rs_anti.getString("accID"));
            otherExact.setDb_key(rs_anti.getString("_Object_key"));
            otherExact.setAccessionKey(rs_anti.getString("_Accession_key"));
            otherExact.setPreferred(rs_anti.getString("preferred"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_ANTIGEN_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'ANTIGEN' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 7";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_antigen = execute(OTHER_ANTIGEN_SEARCH);
        rs_antigen.next();

        writeEnd = new Date();

        log.info("Time taken gather antigen data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_antigen.isAfterLast()) {

            otherExact.setType(rs_antigen.getString("_MGIType_key"));
            otherExact.setData(rs_antigen.getString("accID"));
            otherExact.setDb_key(rs_antigen.getString("_Object_key"));
            otherExact.setAccessionKey(rs_antigen.getString("_Accession_key"));
            otherExact.setPreferred(rs_antigen.getString("preferred"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_EXPERIMENT_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'EXPERIMENT' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key = 4";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_exp = execute(OTHER_EXPERIMENT_SEARCH);
        rs_exp.next();

        writeEnd = new Date();

        log.info("Time taken gather experiment data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_exp.isAfterLast()) {

            otherExact.setType(rs_exp.getString("_MGIType_key"));
            otherExact.setData(rs_exp.getString("accID"));
            otherExact.setDb_key(rs_exp.getString("_Object_key"));
            otherExact.setAccessionKey(rs_exp.getString("_Accession_key"));
            otherExact.setPreferred(rs_exp.getString("preferred"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_IMAGE_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, a._Object_key, 'IMAGE' as _MGIType_key, "
                + "a.preferred" + " FROM ACC_Accession a"
                + " where a.private != 1 and a._MGIType_key =9";

        // Gather the data.

        writeStart = new Date();

        ResultSet rs_image = execute(OTHER_IMAGE_SEARCH);
        rs_image.next();

        writeEnd = new Date();

        log.info("Time taken gather image data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_image.isAfterLast()) {

            otherExact.setType(rs_image.getString("_MGIType_key"));
            otherExact.setData(rs_image.getString("accID"));
            otherExact.setDb_key(rs_image.getString("_Object_key"));
            otherExact.setAccessionKey(rs_image.getString("_Accession_key"));
            otherExact.setPreferred(rs_image.getString("preferred"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_SEQ_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, '"
                + IndexConstants.OTHER_SEQUENCE
                + "' as _MGIType_key, a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a, SEQ_Sequence s"
                + " where a.private != 1 and a._MGIType_key = 19 and a._Object_key = s._Sequence_key and s._Organism_key = 1";

        // Gather the data.

        writeStart = new Date();

        ResultSet rs_seq = execute(OTHER_SEQ_SEARCH);
        rs_seq.next();

        writeEnd = new Date();

        log.info("Time taken gather sequence data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_seq.isAfterLast()) {

            otherExact.setType(rs_seq.getString("_MGIType_key"));
            otherExact.setData(rs_seq.getString("accID"));
            otherExact.setDb_key(rs_seq.getString("_Object_key"));
            otherExact.setAccessionKey(rs_seq.getString("_Accession_key"));
            otherExact.setPreferred(rs_seq.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_seq
                    .getString("_LogicalDB_key")));

            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            otherExact.clear();
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
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

    private void doSequencesByProbe() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_SEQ_BY_PROBE_SEARCH = "SELECT distinct a._Accession_key, "
                + "ac.accID, a._Object_key, 'SEQUENCE' as _MGIType_key, "
                + "a.preferred, ac._LogicalDB_key"
                + " FROM ACC_Accession a, SEQ_Sequence s, "
                + "SEQ_Probe_Cache spc, acc_accession ac"
                + " where a.private != 1 and a._MGIType_key = 19 and "
                + "a._Object_key = s._Sequence_key and s._Organism_key = 1"
                + " and a._Object_key = spc._Sequence_key and "
                + "spc._Probe_key = ac._Object_key and ac._MGIType_key = 3 "
                + "and AC._LogicalDB_key != 9 and ac.private != 1";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_seq_by_probe = execute(OTHER_SEQ_BY_PROBE_SEARCH);
        rs_seq_by_probe.next();

        writeEnd = new Date();

        log.info("Time taken gather sequence by probe id data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it


        while (!rs_seq_by_probe.isAfterLast()) {

            otherExact.setType(rs_seq_by_probe.getString("_MGIType_key"));
            otherExact.setData(rs_seq_by_probe.getString("accID"));
            otherExact.setDb_key(rs_seq_by_probe.getString("_Object_key"));
            otherExact.setAccessionKey(rs_seq_by_probe
                    .getString("_Accession_key"));
            otherExact.setPreferred(rs_seq_by_probe.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_seq_by_probe
                    .getString("_LogicalDB_key")));

            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            otherExact.clear();

            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }

            rs_seq_by_probe.next();
        }

        // Clean up

        System.out
                .println("Done creating documents for sequences by probe IDs!");

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

        String OTHER_SNP_PRIME_SEARCH = "SELECT distinct _Accession_key, "
                + "accID, _Object_key, 'SNP' as _MGIType_key, 1"
                + " FROM SNP_Accession" + " where _MGIType_key = 30";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_snp_prime = executeSnp(OTHER_SNP_PRIME_SEARCH);
        rs_snp_prime.next();

        writeEnd = new Date();

        log.info("Time taken gather snp data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_snp_prime.isAfterLast()) {

            otherExact.setType(rs_snp_prime.getString(4));
            otherExact.setData(rs_snp_prime.getString(2));
            otherExact.setDb_key(rs_snp_prime.getString(3));
            otherExact.setAccessionKey(rs_snp_prime.getString(1));
            otherExact.setPreferred(rs_snp_prime.getString(5));
            // This is an odd case, as far as I can tell, this is hardcoded.
            // In the jsp page that Kim most likely got this requirement
            // from
            // as a result, I'll hardcode it in here as well, with every
            // intention
            // of asking kim for a more rational approach later on.
            otherExact.setProvider("dbSNP");
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_SNP_SECONDARY_SEARCH = "SELECT distinct _Accession_key, "
                + "accID, _Object_key, 'SUBSNP' as _MGIType_key, 0 as prefered"
                + " FROM SNP_Accession" + " where _MGIType_key = 31";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_snp_sub = executeSnp(OTHER_SNP_SECONDARY_SEARCH);
        rs_snp_sub.next();

        writeEnd = new Date();

        log.info("Time taken to gather sub snp data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_snp_sub.isAfterLast()) {

            otherExact.setType(rs_snp_sub.getString("_MGIType_key"));
            otherExact.setData(rs_snp_sub.getString("accID"));
            otherExact.setDb_key(rs_snp_sub.getString("_Object_key"));
            otherExact.setAccessionKey(rs_snp_sub.getString("_Accession_key"));
            otherExact.setPreferred(rs_snp_sub.getString("prefered"));
            // This is the same odd case the the other SNP data.
            otherExact.setProvider("dbSNP");
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
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

        String OTHER_ORTHOLOG_SEARCH = "select distinct a._Accession_key, "
                + "a.accID, nonmouse._Marker_key, 'ORTHOLOG' as _MGIType_key, "
                + "a.preferred, a._LogicalDB_key, m.commonName"
                + " from MRK_Homology_Cache nonmouse, ACC_Accession a, "
                + "MGI_Organism m" + " where nonmouse._Organism_key != 1 and "
                + "nonmouse._Marker_key = a._Object_key"
                + " and a._MGIType_key = 2 and a.private = 0 "
                + "and nonmouse._Organism_key = m._Organism_key";

        // gather the data

        writeStart = new Date();

        ResultSet rs_orthologs = execute(OTHER_ORTHOLOG_SEARCH);
        rs_orthologs.next();

        writeEnd = new Date();

        log.info("Time taken to gather ortholog id data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_orthologs.isAfterLast()) {

            otherExact.setType(rs_orthologs.getString("_MGIType_key"));
            otherExact.setData(rs_orthologs.getString("accID"));
            otherExact.setDb_key(rs_orthologs.getString("_Marker_key"));
            otherExact
                    .setAccessionKey(rs_orthologs.getString("_Accession_key"));
            otherExact.setPreferred(rs_orthologs.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_orthologs
                    .getString("_LogicalDB_key"))
                    + " - " + initCap(rs_orthologs.getString("commonName")));

            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            sis.push(otherExact.getDocument());
            otherExact.clear();

            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            
            rs_orthologs.next();
        }

        // Clean up

        log.info("Done creating documents for ortholog IDs!");
        rs_orthologs.close();

    }

    /**
     * Gather Adult Mouse Anatomy data
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doAMA() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_AMA_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
                + "a._Object_key, 'AMA' as _MGIType_key, "
                + "a.preferred, a._LogicalDB_key"
                + " from acc_accession a, VOC_Term v"
                + " where a.private !=1 and a._MGIType_key = 13 and "
                + "a._Object_key = v._Term_key and v._Vocab_key = 6 "
                + "and a.preferred = 1";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_ama = execute(OTHER_AMA_SEARCH);
        rs_ama.next();

        writeEnd = new Date();

        log.info("Time taken gather ama data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ama.isAfterLast()) {

            otherExact.setType(rs_ama.getString("_MGIType_key"));
            otherExact.setData(rs_ama.getString("accID"));
            otherExact.setDb_key(rs_ama.getString("_Object_key"));
            otherExact.setAccessionKey(rs_ama.getString("_Accession_key"));
            otherExact.setPreferred(rs_ama.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_ama
                    .getString("_LogicalDB_key")));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
            rs_ama.next();
        }

        // Clean up

        log.info("Done creating documents for AMA!");
        rs_ama.close();

    }

    /**
     * Gather the ES Cell Line data.  Please note, we only gather Accession ID's for this data type
     * if they have a direct relationship to an allele.
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doESCellLines() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ES_CELL_LINE_SEARCH = "SELECT distinct a._Accession_key, "
                + "a.accID, aa._Allele_key, 'ESCELL' as _MGIType_key, "
                + "a.preferred, a._LogicalDB_key"
                + " FROM ACC_Accession a, all_allele aa"
                + " where a.private != 1 and a._MGIType_key = 28 and "
                + "a._Object_key = aa._MutantESCellLine_key";

        // Gather the data
        
        writeStart = new Date();

        ResultSet rs_escell = execute(OTHER_ES_CELL_LINE_SEARCH);
        rs_escell.next();

        writeEnd = new Date();

        log.info("Time taken gather es cell line data set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it
        
        while (!rs_escell.isAfterLast()) {
            otherExact.setType(rs_escell.getString("_MGIType_key"));
            otherExact.setData(rs_escell.getString("accID"));
            otherExact.setDb_key(rs_escell.getString("_Allele_key"));
            otherExact.setAccessionKey(rs_escell.getString("_Accession_key"));
            otherExact.setPreferred(rs_escell.getString("preferred"));
            otherExact.setProvider(phmg.get(rs_escell.getString("_LogicalDB_key")));
            otherExact.setDisplay_type("Cell Line ID");
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            sis.push(otherExact.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            otherExact.clear();
            rs_escell.next();
        }

        // Clean up
        
        log.info("Done creating documents for es cell lines!");
        rs_escell.close();
    }
}

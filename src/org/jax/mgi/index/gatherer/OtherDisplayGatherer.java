package org.jax.mgi.index.gatherer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.OtherDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Gatherer responsible for understanding how to gather all needed information
 * that goes into the other display index.
 * 
 * Please note that all "Other" gatherers have a gathering item limit 
 * programmed into each of their subsections. Based on this value, they are
 * only able to put a set maximum number of documents onto the stack before
 * being forced to wait.
 * 
 * This number is a configurable item via the Configuration file.
 * 
 * @author mhall
 * 
 * @has A single instance of an OtherDisplayLuceneDocBuilder, which it uses to
 * create the documents that is puts on the stack.
 * 
 * @does Responsible for running through the algorithm that gathers up all of
 * the data needed to make the other display index. For each subsection, we
 * first gather the date, then we parse through it placing lucene documents on
 * the stack for each row, and then perform cleanup. 
 * 
 * When all of the subsections
 * are complete, we notify the stack that gathering is complete, perform some
 * overall cleanup, and exit.
 */

public class OtherDisplayGatherer extends AbstractGatherer {

    // Class Variables

    private double total = 0;

    /*
     * Since other gathering is so long, it has extra log messages, these
     * variable will control how often these log messages are displayed.
     * 
     * One the output_threshhold has been reached, we output one of these
     * special log messages. We then increment the threshold by the output
     * incrementer, and repeat the process.
     */

    private double output_incrementer = 100000;
    private double output_threshold = 100000;

    // Create the one LuceneDocBuilder that this object will use.

    private OtherDisplayLuceneDocBuilder odldb =
        new OtherDisplayLuceneDocBuilder();

    private Logger log =
        Logger.getLogger(OtherDisplayGatherer.class.getName());
    
    // Create two data variables that are used with timing.

    private Date writeStart;
    private Date writeEnd;
    
    // This class needs a special connection, set it up.
    
    private Connection conSnp;

    public OtherDisplayGatherer(IndexCfg config) {
        super(config);

        try {
            Class.forName(DB_DRIVER);
            String USER = config.get("MGI_PUBLICUSER");
            String PASSWORD = config.get("MGI_PUBLICPASSWORD");
            stack_max = new Integer(config.get("STACK_MAX"));
            log.debug("SNP_JDBC_URL: " + config.get("SNP_JDBC_URL"));
            conSnp = DriverManager.getConnection(config.get("SNP_JDBC_URL"),
                    USER, PASSWORD);
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
     * This method encapsulates the list of tasks that need to be completed in
     * order to gather the information for the OtherDisplay index. Once it is
     * invoked, each object type will be collected in sequence, until all the
     * indexes information is gathered, at which point it sets the gathering
     * state to complete in the shared document stack, and exits.
     */

    public void run() {
        try {

            doProbes();

            doAssays();

            doReferences();

            doSequences();

            doAlleles();

            doOrthologs();

            doAntibodies();

            doAntigens();

            doExperiments();

            doImages();

            doSnps();

            doSubSnps();

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
     * Gather the probe data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doProbes() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_PROBE_DISPLAY_KEY = "select distinct _Probe_key, '"
                + IndexConstants.OTHER_PROBE + "' as type, name, vt.Term"
                + " from PRB_Probe_view, voc_term vt"
                + " where _SegmentType_key = vt._Term_key";

        // Gather the data

        writeStart = new Date();

        ResultSet rs = execute(OTHER_PROBE_DISPLAY_KEY);
        rs.next();

        writeEnd = new Date();

        log.info("Time taken gather probe result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs.isAfterLast()) {

            odldb.setDb_key(rs.getString("_Probe_key"));
            odldb.setDataType(rs.getString("type"));
            odldb.setQualifier(rs.getString("Term"));
            odldb.setName(rs.getString("name"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs.next();
        }

        // Clean up

        log.info("Done Probes!");
        rs.close();

    }

    /**
     * Gather the assay data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAssays() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ASSAY_DISPLAY_KEY = "select _Assay_key, '"
                + IndexConstants.OTHER_ASSAY
                + "' as type, name, symbol from GXD_Assay_View";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_assay = execute(OTHER_ASSAY_DISPLAY_KEY);
        writeEnd = new Date();
        rs_assay.next();
        log.info("Time taken gather ASSAY result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_assay.isAfterLast()) {

            odldb.setDb_key(rs_assay.getString("_Assay_key"));
            odldb.setDataType(rs_assay.getString("type"));
            odldb.setName(rs_assay.getString("symbol") + ", "
                    + rs_assay.getString("name"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug ("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_assay.next();
        }

        // Clean up

        log.info("Done Assay!");
        rs_assay.close();
    }

    /**
     * Gather the reference data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doReferences() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_REF_DISPLAY_KEY = "select distinct _Refs_key, '"
                + IndexConstants.OTHER_REFERENCE
                + "' as type, short_citation from BIB_View";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_ref = execute(OTHER_REF_DISPLAY_KEY);

        writeEnd = new Date();

        rs_ref.next();

        log.info("Time taken gather reference result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ref.isAfterLast()) {

            odldb.setDb_key(rs_ref.getString("_Refs_key"));
            odldb.setDataType(rs_ref.getString("type"));
            odldb.setName(rs_ref.getString("short_citation"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_ref.next();
        }

        // Clean up

        log.info("Done References!");
        rs_ref.close();

    }

    /**
     * Gather the sequence data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSequences() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_SEQ_DISPLAY_KEY = "select distinct _Sequence_key, '"
                + IndexConstants.OTHER_SEQUENCE
                + "' as type, description, sequenceType"
                + " from SEQ_Sequence_view";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_seq = execute(OTHER_SEQ_DISPLAY_KEY);

        writeEnd = new Date();

        rs_seq.next();
        log.info("Time taken gather sequence result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_seq.isAfterLast()) {

            odldb.setDb_key(rs_seq.getString("_Sequence_key"));
            odldb.setDataType(rs_seq.getString("type"));
            odldb.setQualifier(rs_seq.getString("sequenceType"));
            if (rs_seq.getString("description") != null) {
                odldb.setName(rs_seq.getString("description"));
            }
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place a document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_seq.next();
        }

        // Clean up

        log.info("Done Sequences!");
        rs_seq.close();
    }

    /**
     * Gather the alleles data.  Please note that this has a realized display
     * field.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAlleles() throws SQLException, InterruptedException {

        // SQL for this Subsection.

        String OTHER_ALL_DISPLAY_KEY = "select distinct av._Allele_key, '"
                + IndexConstants.OTHER_ALLELE
                + "' as type, av.symbol, av.name, ml.label, vt.term"
                + " from ALL_Allele_View av, VOC_Term vt, MRK_Label ml"
                + " where av._Allele_Type_key = vt._Term_key and"
                + " av._Marker_key = ml._Marker_key"
                + " and ml._Label_Status_key = 1 and ml.labelType = 'MN'";

        // Gather the data.

        writeStart = new Date();
        ResultSet rs_all = execute(OTHER_ALL_DISPLAY_KEY);
        writeEnd = new Date();
        rs_all.next();
        log.info("Time taken gather allele result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse the data

        while (!rs_all.isAfterLast()) {

            odldb.setDb_key(rs_all.getString("_Allele_key"));
            odldb.setDataType(rs_all.getString("type"));
            odldb.setQualifier(rs_all.getString("term"));
            
            // The name for Alleles is a realized field.
            
            String symbol = new String(rs_all.getString("symbol"));
            String name = new String(rs_all.getString("name"));
            String marker_name = new String(rs_all.getString("label"));

            odldb.setName(symbol + ", " + marker_name + "; " + name);
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_all.next();
        }

        // Clean up

        log.info("Done Alleles!");
        rs_all.close();

    }

    /**
     * Gather Ortholog data.  Please note that this has a realized display 
     * field.
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doOrthologs() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ORTHOLOG_DISPLAY = "select _Marker_key, '"
                + IndexConstants.OTHER_ORTHOLOG + "' as type, symbol, name"
                + " from MRK_Marker_View" + " where _Organism_key != 1";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_ortho = execute(OTHER_ORTHOLOG_DISPLAY);
        writeEnd = new Date();
        rs_ortho.next();
        log.info("Time taken gather ortholog result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ortho.isAfterLast()) {

            odldb.setDb_key(rs_ortho.getString("_Marker_key"));
            odldb.setDataType(rs_ortho.getString("type"));
            
            // The name for Orthologs is a realized field.
            
            String symbol = new String(rs_ortho.getString("symbol"));
            String name = new String(rs_ortho.getString("name"));

            odldb.setName(symbol + ", " + name);
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_ortho.next();
        }

        // Clean up

        log.info("Done Orthologs!");
        rs_ortho.close();
    }

    /**
     * Gather the antibody data.
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAntibodies() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ANTIBODY_DISPLAY_KEY = "select distinct _Antibody_key, '"
                + IndexConstants.OTHER_ANTIBODY
                + "' as type, antibodyName from GXD_Antibody";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_antibody = execute(OTHER_ANTIBODY_DISPLAY_KEY);
        writeEnd = new Date();
        rs_antibody.next();
        log.info("Time taken gather antibody result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_antibody.isAfterLast()) {

            odldb.setDb_key(rs_antibody.getString("_Antibody_key"));
            odldb.setDataType(rs_antibody.getString("type"));
            odldb.setName(rs_antibody.getString("antibodyName"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_antibody.next();
        }

        // Clean up

        log.info("Done Antibodies!");
        rs_antibody.close();
    }

    /**
     * Gather the antigen data.
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAntigens() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ANTIGEN_DISPLAY_KEY = "select distinct _Antigen_key, '"
                + IndexConstants.OTHER_ANTIGEN
                + "' as type, antigenName from GXD_Antigen";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_antigen = execute(OTHER_ANTIGEN_DISPLAY_KEY);
        writeEnd = new Date();
        rs_antigen.next();
        log.info("Time taken gather antigen result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_antigen.isAfterLast()) {

            odldb.setDb_key(rs_antigen.getString("_Antigen_key"));
            odldb.setDataType(rs_antigen.getString("type"));
            odldb.setName(rs_antigen.getString("antigenName"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_antigen.next();
        }

        // Clean up

        log.info("Done Antigen!");
        rs_antigen.close();
    }

    /**
     * Gather experiment data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doExperiments() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_EXPERIMENT_DISPLAY_KEY = "select _Expt_key, '"
                + IndexConstants.OTHER_EXPERIMENT
                + "' as type, short_citation, exptType from MLD_Expt_View";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_experiment = execute(OTHER_EXPERIMENT_DISPLAY_KEY);
        writeEnd = new Date();
        rs_experiment.next();
        log.info("Time taken gather experiment result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_experiment.isAfterLast()) {

            odldb.setDb_key(rs_experiment.getString("_Expt_key"));
            odldb.setDataType(rs_experiment.getString("type"));
            odldb.setQualifier(rs_experiment.getString("exptType"));
            odldb.setName(rs_experiment.getString("short_citation"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_experiment.next();
        }

        // Clean up

        log.info("Done Experiments!");
        rs_experiment.close();
    }

    /**
     * Gather the image data.
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doImages() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_IMAGE_DISPLAY_KEY = "select _Image_key, '"
                + IndexConstants.OTHER_IMAGE
                + "' as type, short_citation  from IMG_Image_View";

        // Gather the data

        ResultSet rs_image = execute(OTHER_IMAGE_DISPLAY_KEY);
        writeEnd = new Date();
        rs_image.next();
        log.info("Time taken gather image result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // parse it

        while (!rs_image.isAfterLast()) {

            odldb.setDb_key(rs_image.getString("_Image_key"));
            odldb.setDataType(rs_image.getString("type"));
            odldb.setName(rs_image.getString("short_citation"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_image.next();
        }

        // Clean up

        log.info("Done Images!");
        rs_image.close();
    }

    /**
     * Collect Ref Snp data. Please note that it has a realized name field.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSnps() throws SQLException, InterruptedException {

        // SQL for this Subsection

        writeStart = new Date();
        
        String OTHER_SNP_PRIME_DISPLAY = "select _ConsensusSnp_key, '"
                + IndexConstants.OTHER_SNP
                + "' as type, chromosome, startCoordinate "
                + "from SNP_Coord_Cache ";

        // Gather the data

        ResultSet rs_snp = executeSnp(OTHER_SNP_PRIME_DISPLAY);
        writeEnd = new Date();
        rs_snp.next();
        log.info("Time taken gather SNP result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // parse it

        while (!rs_snp.isAfterLast()) {

            odldb.setDb_key(rs_snp.getString("_ConsensusSnp_key"));
            odldb.setDataType(rs_snp.getString("type"));

            // SNPs are a bit different, they need a realized name field.

            odldb.setName("SNP at Chr" + rs_snp.getString("chromosome")
                    + ":" + rs_snp.getInt("startCoordinate"));

            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_snp.next();
        }

        // Clean up

        log.info("Done SNPs!");
        rs_snp.close();

    }

    /**
     * Collect the sub SNP data. Please note that this type's name field is a
     * realized field.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSubSnps() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_SNP_SECONDARY_DISPLAY = "select ss._SubSNP_Key, '"
                + IndexConstants.OTHER_SUBSNP
                + "' as type, sc.chromosome, sc.startCoordinate"
                + " from SNP_Coord_Cache sc, SNP_Subsnp ss"
                + " where ss._ConsensusSnp_key = sc._ConsensusSnp_key";

        // Gather the data

        ResultSet rs_subsnp = executeSnp(OTHER_SNP_SECONDARY_DISPLAY);
        writeEnd = new Date();
        rs_subsnp.next();
        log.info("Time taken gather Sub SNP result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_subsnp.isAfterLast()) {

            odldb.setDb_key(rs_subsnp.getString("_SubSNP_Key"));
            odldb.setDataType(rs_subsnp.getString("type"));

            // SNPs are a bit different, they need a realized name field.

            odldb.setName("SNP at Chr"
                    + rs_subsnp.getString("chromosome") + ":"
                    + rs_subsnp.getInt("startCoordinate"));

            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_subsnp.next();
        }

        // Clean up

        log.info("Done Sub SNPs!");
        rs_subsnp.close();
    }

    /**
     * Collect the Adult Mouse Anatomy data.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doAMA() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_AMA_DISPLAY = "select _Term_key, '"
                + IndexConstants.OTHER_AMA + "' as type, term"
                + " from VOC_Term" + " where _Vocab_key = 6 ";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_ama = execute(OTHER_AMA_DISPLAY);
        writeEnd = new Date();
        rs_ama.next();
        log.info("Time taken gather AMA result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_ama.isAfterLast()) {

            odldb.setDb_key(rs_ama.getString("_Term_key"));
            odldb.setDataType(rs_ama.getString("type"));
            odldb.setName(rs_ama.getString("term"));
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_ama.next();
        }

        // Clean up

        log.info("Done AMA!");
        rs_ama.close();
    }

    /**
     * Gather ES Cell Line -> Allele relationship data.  Please note that this
     * also has a realized display field.
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doESCellLines() throws SQLException, InterruptedException {

        // SQL for this Subsection

        String OTHER_ES_CELL_LINE_DISPLAY = "select distinct av._Allele_key, '"
                + IndexConstants.OTHER_ESCELL
                + "' as type, av.symbol, av.name, ml.label, vt.term"
                + " from ALL_Allele_View av, VOC_Term vt, "
                + "MRK_Label ml, acc_accession ac"
                + " where av._Allele_Type_key = vt._Term_key "
                + "and av._Marker_key = ml._Marker_key and "
                + "ml._Label_Status_key = 1"
                + " and ac._MGIType_key = 28 and ac.private != 1 "
                + "and ac._Object_key = av._MutantESCellLine_key"
                + " and ml.labelType = 'MN'";

        // Gather the data

        writeStart = new Date();
        ResultSet rs_es_cell = execute(OTHER_ES_CELL_LINE_DISPLAY);
        writeEnd = new Date();
        rs_es_cell.next();
        log.info("Time taken gather es cell line result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_es_cell.isAfterLast()) {

            odldb.setDb_key(rs_es_cell.getString("_Allele_key"));
            odldb.setDataType(rs_es_cell.getString("type"));
            odldb.setQualifier(rs_es_cell.getString("term"));
            
            // ESCellLines have a realized name field.
            
            String symbol = new String(rs_es_cell.getString("symbol"));
            String name = new String(rs_es_cell.getString("name"));
            String marker_name = new String(rs_es_cell.getString("label"));

            odldb.setName(symbol + ", " + marker_name + "; " + name);
            while (sis.size() > stack_max) {
                Thread.sleep(1);
            }
            
            // Place the document on the stack.
            
            sis.push(odldb.getDocument());
            total++;
            if (total >= output_threshold) {
                log.debug("We have now gathered " + total
                        + " documents!");
                output_threshold += output_incrementer;
            }
            odldb.clear();
            rs_es_cell.next();
        }

        // Clean up

        log.info("Done ES Cell Lines!");
        rs_es_cell.close();
    }
}

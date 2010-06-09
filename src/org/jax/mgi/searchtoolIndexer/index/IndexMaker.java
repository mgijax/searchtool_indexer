package org.jax.mgi.searchtoolIndexer.index;

import java.io.File;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.jax.mgi.searchtoolIndexer.gatherer.AbstractGatherer;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.MGIAnalyzer;
import org.jax.mgi.shr.searchtool.MGITokenAnalyzer;
import org.jax.mgi.shr.searchtool.StemmedMGIAnalyzer;

/**
 * Main Indexing Class for the MGI Search tool. This will accept command line
 * arguments in order to overwrite the default configuration items.
 *
 * <br>
 * They are set as follows: IndexMaker directory indexCode
 *
 * <br>
 * directory = Directory to put index to.
 * indexCode = What do you want to index? Current choices are:
 * g (genes)
 * ge (gene exact)
 * gd (gene display)
 * ga (gene accession ID's)
 * gs (gene symbols)
 * gve (gene related vocab exact information)
 * gva (gene related vocab accession id's)
 * gvd (Gene related vocab dag information)
 * v (vocab)
 * vd (vocab display)
 * ve (vocab exact)
 * va (vocab accession id's)
 * o (other)
 * od (other display)
 * t (non id tokens)
 *
 *
 * @author mhall
 *
 * @has A IndexCfg Object, which contains all of the configurable information
 * for this script.
 *
 * A gatherer thread, which implements the AbstractGatherer class, this thread
 * is what populates the stack for indexing.
 *
 * A Consumer thread, which implements the IndexController class.  This thread
 * coordinates all the Indexers in their effort to empty the shared document stack.
 *
 * @does Upon instantiation it reads in the arguments that have been passed
 * to it from the command line, and then proceeds onto setting up the specific
 * indexing task its been asked to perform.
 *
 * After indexing has completed, it prints out a timing report and exits.
 *
 */

public class IndexMaker {

    // private static Configuration config;

    private static IndexCfg config;

    // The Default index directory.
    private static File INDEX_DIR = null;

    private static IndexWriter writer;

    private static Thread gatherer = null;

    private static Thread consumer = null;

    private static Logger log = Logger.getLogger(IndexMaker.class.getName());

    /*
     * This sections defines variables that will be used by the Lucene index
     * to control its indexing behavior.  For more information please see the
     * Lucene javadocs.  These are configured for our use via the
     * configuration object in the setup method.
     */

    private static int MERGE_FACTOR;

    private static int MAX_BUFFERED_DOCS;

    private static boolean USE_COMPOUND_DOCS;

    // Main Method

    public static void main(String[] args) {

        // Read in config, and setup for indexing.

        setup(args);

        Date startOverall = new Date();

        // Start the specific data gathering thread for our task.
        // (Implements AbstractGatherer)

        gatherer.start();

        // Start the consumer (IndexController) thread, which
        // takes documents off of the SharedDocumentStack.

        consumer.start();

        // Wait for the consumer to finish, and then print out the
        // overall time report.

        try {
            consumer.join();
        } catch (Exception e) {
            log.error(e);
            System.exit(1);
        }

        Date endOverall = new Date();

        timeReport(startOverall, endOverall);

    }

    /*
     * Internal method, used to get a copy of the configuration from the
     * environment.
     */

    private static void getConfig() {
        try {

            config = new IndexCfg();

            MAX_BUFFERED_DOCS = new Integer(config.get("MAX_BUFFERED_DOCS"))
                    .intValue();
            MERGE_FACTOR = new Integer(config.get("MERGE_FACTOR")).intValue();
            USE_COMPOUND_DOCS = new Boolean(config.get("USE_COMPOUND_DOCS"));

        } catch (Exception e) {
            log.error(e);
        }
    }

    /*
     * Internal method used to print out a time report for this class.
     */

    private static void timeReport(Date start, Date end) {
        long timeDiff = end.getTime() - start.getTime();
        double seconds = (timeDiff / 1000.0);
        double minutes = seconds / 60.0;

        log.info("=================================================");
        log.info("Completed Indexing");
        log.info("Total Time Taken: ");
        log.info("(" + timeDiff + ") Milliseconds Total");
        log.info("(" + seconds + ") Seconds Total");
        log.info("(" + minutes + ") Minutes Total");
        log.info("=================================================");

    }

    /*
     * Internal method used to setup the entire indexing process.
     */

    private static void setup(String[] args) {

        // Verify that we have enough arguments to this code.
        // If we do not we abort processing.

        if (args.length != 2) {
            log.error("You must supply two arguments to this script.");
            log.error("IndexDir is the first, which should be a path"
                    + " indexes you are trying to create.");
            log.error("IndexCode: The code to the index you"
                    + " are trying to create.");
            System.exit(1);
        }

        // Get a local copy of the configuration

        getConfig();

        // Set up the gathererMap.

        HashMap<String, String> gathererMap = new HashMap<String, String>();

        String gathererPackage = "org.jax.mgi.searchtoolIndexer.gatherer.";

        gathererMap.put("g", gathererPackage + "GenomeFeatureInexactGatherer");
        gathererMap.put("ge", gathererPackage + "GenomeFeatureExactGatherer");
        gathererMap.put("ga", gathererPackage + "GenomeFeatureAccIDGatherer");
        gathererMap.put("gs", gathererPackage + "GenomeFeatureSymbolGatherer");
        gathererMap.put("gd", gathererPackage + "GenomeFeatureDisplayGatherer");
        gathererMap.put("gva", gathererPackage + "GenomeFeatureVocabAccIDGatherer");
        gathererMap.put("gve", gathererPackage + "GenomeFeatureVocabExactGatherer");
        gathererMap.put("gvd", gathererPackage + "GenomeFeatureVocabDagGatherer");
        gathererMap.put("v", gathererPackage + "VocabInexactGatherer");
        gathererMap.put("ve", gathererPackage + "VocabExactGatherer");
        gathererMap.put("va", gathererPackage + "VocabAccIDGatherer");
        gathererMap.put("vd", gathererPackage + "VocabDisplayGatherer");
        gathererMap.put("t", gathererPackage + "NonIDTokenGatherer");
        gathererMap.put("o", gathererPackage + "OtherExactGatherer");
        gathererMap.put("od", gathererPackage + "OtherDisplayGatherer");


        try {

            // Set up our specific gatherer for the index we want to create.
            // We do this via reflection.

            if (! gathererMap.containsKey(args[1].toLowerCase())) {
                log.error("You have requested to create an index that doesn't");
                log.error(" exist.  Please check your arguments and try again.");
                System.exit(1);
            }

            gatherer = new Thread((AbstractGatherer) Class.forName(
                    gathererMap.get(args[1].toLowerCase())).getConstructor(
                    IndexCfg.class).newInstance(config));

            log.info("Creating " + gathererMap.get(args[1].toLowerCase())
                    + " index.");

            // Set the index location to whatever the second command line
            // argument is.

            INDEX_DIR = new File(args[0]);

            // Create a new indexWriter, using the MGIAnalyzer Wrapper,
            // the MGITokenAnalyzer, or the StandardAnalayzer.

            // Inexact genes and vocab indexes use a multi
            // column approach, which means we have to have a more complex
            // Analyzer type, and AnalyzerWrapper.  This allows us to specify
            // on a per field basis which Analyzer to use.
            // In this case we use the MGIAnalyzer for the unstemmed datafield
            // and the StemmedMGIAnalyzer for the stemmed field.

            if (args[1].toLowerCase().equals("g")
                    || args[1].toLowerCase().equals("v")) {

                // Set up our customized Analyzer Wrapper

                PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(
                        new StandardAnalyzer());
                aWrapper.addAnalyzer("data", new MGIAnalyzer());
                aWrapper.addAnalyzer("sdata", new StemmedMGIAnalyzer());
                writer = new IndexWriter(INDEX_DIR, aWrapper, true);

            } else if (args[1].toLowerCase().equals("t")) {

                // Use a special Analyzer, which breaks the input up on white
                // space, so we can get a listing of all the large tokens
                // across all of the indexes.

                writer = new IndexWriter(INDEX_DIR, new MGITokenAnalyzer(),
                        true);
            } else {

                // If we aren't in a special analyzer case, use the standard
                // one instead.

                writer = new IndexWriter(INDEX_DIR, new StandardAnalyzer(),
                        true);
            }

            // Set the various configurable Lucene values

            // This controls how many physical files will be created on the
            // filesystem before a merge occurs.
            writer.setMergeFactor(MERGE_FACTOR);

            // How many documents will the IndexWriter buffer before flushing
            // them to disk.
            writer.setMaxBufferedDocs(MAX_BUFFERED_DOCS);

            // When the index is optimized collapse the files on the
            // filesystem as much as possible.
            writer.setUseCompoundFile(USE_COMPOUND_DOCS);

            // Initialize the consumer (IndexController)
            consumer = new Thread(new IndexController(writer));

        } catch (Exception e) {
            log.error(e);
            System.exit(1);
        }
    }

}

package org.jax.mgi.index.index;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.jax.mgi.index.gatherer.MarkerAccIDGatherer;
import org.jax.mgi.index.gatherer.MarkerDisplayGatherer;
import org.jax.mgi.index.gatherer.MarkerExactGatherer;
import org.jax.mgi.index.gatherer.MarkerInexactGatherer;
import org.jax.mgi.index.gatherer.MarkerSymbolGatherer;
import org.jax.mgi.index.gatherer.MarkerVocabAccIDGatherer;
import org.jax.mgi.index.gatherer.MarkerVocabDagGatherer;
import org.jax.mgi.index.gatherer.MarkerVocabExactGatherer;
import org.jax.mgi.index.gatherer.NonIDTokenGatherer;
import org.jax.mgi.index.gatherer.OtherDisplayGatherer;
import org.jax.mgi.index.gatherer.OtherExactGatherer;
import org.jax.mgi.index.gatherer.VocabAccIDGatherer;
import org.jax.mgi.index.gatherer.VocabDisplayGatherer;
import org.jax.mgi.index.gatherer.VocabExactGatherer;
import org.jax.mgi.index.gatherer.VocabInexactGatherer;
import org.jax.mgi.shr.config.Configuration;

import QS_Commons.MGIAnalyzer;
import QS_Commons.MGITokenAnalyzer;
import QS_Commons.StemmedMGIAnalyzer;

/**
 * Main Indexing Class for the MGI Search tool. This will accept command line
 * arguments in order to overwrite the default configuration items.
 * 
 * <br>
 * They are set as follows: ThreadProto zzzzzz name
 * 
 * <br>
 * zzzzz = Directory to put index to. Default is
 * /home/mhall/Search/index/index2_jdbc <br>
 * name = What do you want to index? Current choices are g (genes) ge (gene
 * exact) gd (gene display), ga (gene accession ID's), gs (gene symbols) v
 * (vocab), vd (vocab display), ve (vocab exact) o (other) and od (other
 * display)
 * 
 * <br>
 * <br>
 * You can accept the defaults for any value by simply passing the word null as
 * the argument.
 * 
 * @author mhall
 * 
 * 
 */

public class MakeIndex {

    // Main Method
    
    public static void main(String[] args) {


        
        // Read in config, and setup for indexing.

        setup(args);

        Date startOverall = new Date();

        // Start the specific data gathering thread for our task.

        gatherer.start();

        try {
            // gatherer.join(1000);
            consumer.start();
            // Wait for the consumer to finish, and then print out the
            // overall time report.
            consumer.join();
        } catch (Exception e) {
            log.error(e);
        }

        Date endOverall = new Date();

        timeReport(startOverall, endOverall);

    }

    /*
     * Internal method, used to get a copy of the GlobalConfig
     */

    private static void getConfig() {
        try {
            config = Configuration.load("Configuration", false);
            INDEX_DIR = new File(config.get("INDEX_DIR"));
            MERGE_FACTOR = new Integer(config.get("MERGE_FACTOR")).intValue();
            MAX_BUFFERED_DOCS = new Integer(config.get("MAX_BUFFERED_DOCS"))
                    .intValue();
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
        log.info("("+timeDiff+") Milliseconds Total");
        log.info("(" + seconds + ") Seconds Total");
        log.info("("+minutes + ") Minutes Total");
        log.info("=================================================");

    }

    /*
     * Internal method used to setup the entire indexing process.
     */

    private static void setup(String[] args) {

        // Get a local copy of the configuration

        getConfig();

        // Set up our customized MGI Analyzer.

        PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(
                new StandardAnalyzer());
        aWrapper.addAnalyzer("data", new MGIAnalyzer());
        aWrapper.addAnalyzer("sdata", new StemmedMGIAnalyzer());

        // Figure out what type of index we are trying to build, from the
        // arguments pass via the command line.
        
        switch (args.length) {
        case 2:
            if (!args[1].equals("null")) {
                if (args[1].toLowerCase().equals("g")) {
                    gatherer = new Thread(new MarkerInexactGatherer(config));
                    log.info("Indexing Gene's/Markers/Vocabularies in a single gene approach using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("v")) {
                    gatherer = new Thread(new VocabInexactGatherer(config));
                    log.info("Indexing Vocab Information by field into a single index Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("gd")) {
                    gatherer = new Thread(new MarkerDisplayGatherer(config));
                    log.info("Indexing Gene's/Markers Display Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("ge")) {
                    gatherer = new Thread(new MarkerExactGatherer(config));
                    log.info("Indexing Gene's/Markers Exact Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("ga")) {
                    gatherer = new Thread(new MarkerAccIDGatherer(config));
                    log.info("Indexing Gene's/Markers Accession ID Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("gs")) {
                    gatherer = new Thread(new MarkerSymbolGatherer(config));
                    log.info("Indexing Gene's/Markers Symbol Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("gva")) {
                    gatherer = new Thread(new MarkerVocabAccIDGatherer(config));
                    log.info("Indexing Genes/Marker's Vocab Accession ID Information!");
                } else if (args[1].toLowerCase().equals("gve")) {
                    gatherer = new Thread(new MarkerVocabExactGatherer(config));
                    log.info("Indexing Gene's/Markers Vocab Exact");
                } else if (args[1].toLowerCase().equals("gvd")) {
                    gatherer = new Thread(new MarkerVocabDagGatherer(config));
                    log.info("Indexing Gene's/Markers Vocab DAG Information");
                } else if (args[1].toLowerCase().equals("vd")) {
                    gatherer = new Thread(new VocabDisplayGatherer(config));
                    log.info("Indexing Vocab Display Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("ve")) {
                    gatherer = new Thread(new VocabExactGatherer(config));
                    log.info("Indexing Vocab Exact Match Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("va")) {
                    gatherer = new Thread(new VocabAccIDGatherer(config));
                    log.info("Indexing Vocab Accession ID Match Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("o")) {
                    gatherer = new Thread(new OtherExactGatherer(config));
                    log.info("Indexing Accession (Other) Exact Match Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("od")) {
                    gatherer = new Thread(new OtherDisplayGatherer(config));
                    log.info("Indexing Accession (Other) Display Information using Standard Analyzer!");
                } else if (args[1].toLowerCase().equals("t")) {
                    gatherer = new Thread(new NonIDTokenGatherer(config));
                    log.info("Indexing Non ID Token Information using Standard Analyzer!");
                }
            }
        case 1:
            // If we are passed a second argument on the command line, overwrite
            // the default index
            // location to be whatever the second argument is.

            if (!args[0].equals("null")) {
                INDEX_DIR = new File(args[0]);
            }
            ;

            // Create a new indexWriter, using the MGIAnalyzer Wrapper,
            // the MGITokenAnalyzer, or the StandardAnalayzer.

            if (args[1].toLowerCase().equals("g")
                    || args[1].toLowerCase().equals("v")) {
                try {
                    log.debug("Using the MGIAnalyzer Analyzer");
                    writer = new IndexWriter(INDEX_DIR, aWrapper, true);

                } catch (IOException e) {
                    log.error(e);
                }
            } else if (args[1].toLowerCase().equals("t")) {
                
                try {
                    writer = new IndexWriter(INDEX_DIR, new MGITokenAnalyzer(),
                            true);
                } catch (IOException e) {
                    log.error(e);
                }
            } 
            else {
                
                // If we aren't in a special analyzer case, use the standard one instead.
                
                try {
                    writer = new IndexWriter(INDEX_DIR, new StandardAnalyzer(),
                            true);
                } catch (IOException e) {
                    log.error(e);
                }
            }
            
            writer.setMergeFactor(MERGE_FACTOR);
            writer.setMaxBufferedDocs(MAX_BUFFERED_DOCS);
            writer.setUseCompoundFile(USE_COMPOUND_DOCS);

            // Initialize the consumer (IndexController)

            consumer = new Thread(new IndexController(writer));
        default:
        }
    }

    private static Configuration config;

    // The Default index directory.
    private static File          INDEX_DIR = new File("./index/index");
    
    private static IndexWriter   writer;

    private static Thread        gatherer  = null;

    private static Thread        consumer  = null;

    private static Logger log = Logger.getLogger(MakeIndex.class.getName());

    /* 
     * This sections defines variables that will be used by the Lucene index to control
     * its indexing behavior.  For more information please see the Lucene javadocs.
     * These are configured for our use via the configuration object in the setup method.
     */
    
    private static int           MERGE_FACTOR;

    private static int           MAX_BUFFERED_DOCS;

    private static boolean       USE_COMPOUND_DOCS;


}

package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we might 
 * need for the marker symbol index. This is therefore restricted to just 
 * symbols for markers/alleles and orthologs.
 * 
 * @author mhall
 * 
 * @has A hashmap used to translate from shortened codes ("AN") to English 
 * words ("Allele Name") 
 * 
 * An instance of the provider hash map gatherer, which
 * translates from logical Db's -> Display Strings 
 * 
 * A single instance of the
 * MarkerExactLuceneDocBuilder which is used throughout to create its needed
 * Lucene documents
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts 
 * parsing through its result set. For each record, we generate a Lucene 
 * document, and place it on the shared stack.
 * 
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class MarkerSymbolGatherer extends AbstractGatherer {

    // Class Variables

    private HashMap<String, String> hm = new HashMap<String, String>();

    private MarkerExactLuceneDocBuilder meldb =
        new MarkerExactLuceneDocBuilder();

    private Logger log = Logger.getLogger(MarkerAccIDGatherer.class.getName());
    
    private Date writeStart;
    private Date writeEnd;

    /**
     * Create a new instance of the MarkerExactGatherer, and populate its
     * translation hashmaps.
     * 
     * @param config
     */

    public MarkerSymbolGatherer(IndexCfg config) {

        super(config);

        hm.put(IndexConstants.MARKER_SYMBOL, "Symbol");
        hm.put(IndexConstants.ALLELE_SYMBOL, "Allele Symbol");
        hm.put(IndexConstants.ORTHOLOG_SYMBOL, "Ortholog Symbol");

    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create a MarkerExact document.
     */

    public void run() {
        try {

            doLabels();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Grab all labels associated with markers (Alleles and orthologs included)
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doLabels() throws SQLException, InterruptedException {

        // SQL For this Subsection

        String GENE_LABEL_EXACT = "select ml._Marker_key, "
                + "ml.label, ml._OrthologOrganism_key, ml.labelType,"
                + " ml.labelTypeName, ml._Label_Status_key," + " ml._Label_key"
                + " from MRK_Label ml, MRK_Marker m"
                + " where  ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2"
                + "and ml.labelType in ('MS', 'AS', 'OS')";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_label = execute(GENE_LABEL_EXACT);
        rs_label.next();

        writeEnd = new Date();

        log.info("Time taken to gather label's result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        String displayType = "";
        
        while (!rs_label.isAfterLast()) {

            if (rs_label.getString("labelType").equals(
                    IndexConstants.ORTHOLOG_SYMBOL)) {
                String organism = rs_label.getString("_OrthologOrganism_key");
                
                // There is a special case where we want to define a new type
                // for human and rat symbols.
                
                if (organism != null && organism.equals("2")) {
                    meldb.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
                }
                else if (organism != null && organism.equals("44")) {
                    meldb.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_RAT);
                }
                else {
                    meldb.setDataType(rs_label.getString("labelType"));  
                }
            }
            else {
                meldb.setDataType(rs_label.getString("labelType"));
            }
            
            // If we have an old symbol, we need to create a custom type.
            
            if (!rs_label.getString("_Label_Status_key").equals("1")) {
                
                meldb.setDataType(meldb.getDataType()+"O");
            }
            
            meldb.setData(rs_label.getString("label"));
            meldb.setDb_key(rs_label.getString("_Marker_key"));
            meldb.setUnique_key(rs_label.getString("_Label_key")
                    + IndexConstants.MARKER_TYPE_NAME);
            displayType = initCap(rs_label.getString("labelTypeName"));
            if (displayType.equals("Current Symbol")) {
                displayType = "Symbol";
            }
            meldb.setDisplay_type(displayType);
            
            // Place the document on the stack.
            
            sis.push(meldb.getDocument());
            meldb.clear();
            rs_label.next();
        }

        // Clean up

        rs_label.close();

        log.info("Done Labels!");
    }
}

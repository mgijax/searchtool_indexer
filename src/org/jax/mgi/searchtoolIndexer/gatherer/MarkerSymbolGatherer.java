package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.MarkerSymbolLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we might 
 * need for the marker symbol index. This is therefore restricted to just 
 * symbols for markers/alleles and orthologs.
 * 
 * This information is then used to populate the markerSymbol index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts 
 * parsing through its result set. For each record, we generate a Lucene 
 * document, and place it on the shared stack.
 * 
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class MarkerSymbolGatherer extends DatabaseGatherer {

    // Class Variables


    private MarkerSymbolLuceneDocBuilder builder =
        new MarkerSymbolLuceneDocBuilder();

    /**
     * Create a new instance of the MarkerExactGatherer, and populate its
     * translation hashmaps.
     * 
     * @param config
     */

    public MarkerSymbolGatherer(IndexCfg config) {
        super(config);
    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create a MarkerExact document.
     */

    public void runLocal() throws Exception {
            doSymbols();
    }

    /**
     * Grab all labels associated with markers (Alleles and orthologs included)
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doSymbols() throws SQLException, InterruptedException {

        // Gather up all the marker, allele and ortholog symbols, where the 
        // symbol is for mouse, and the marker has not been withdrawn.
        
        String GENE_LABEL_EXACT = "select ml._Marker_key, "
                + "ml.label, ml._OrthologOrganism_key, ml.labelType,"
                + " ml.labelTypeName, ml._Label_Status_key," + " ml._Label_key"
                + " from MRK_Label ml, MRK_Marker m"
                + " where  ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2"
                + "and ml.labelType in ('MS', 'AS', 'OS')";

        // Gather the data

        ResultSet rs_label = executor.executeMGD(GENE_LABEL_EXACT);
        rs_label.next();

        log.info("Time taken to gather label's result set: "
                + executor.getTiming());

        // Parse it

        String displayType = "";
        
        while (!rs_label.isAfterLast()) {

            if (rs_label.getString("labelType").equals(
                    IndexConstants.ORTHOLOG_SYMBOL)) {
                String organism = rs_label.getString("_OrthologOrganism_key");
                
                // There is a special case where we want to define a new type
                // for human and rat symbols.
                
                if (organism != null && organism.equals("2")) {
                    builder.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
                }
                else if (organism != null && organism.equals("40")) {
                    builder.setDataType(
                            IndexConstants.ORTHOLOG_SYMBOL_RAT);
                }
                else {
                    builder.setDataType(rs_label.getString("labelType"));  
                }
            }
            else {
                builder.setDataType(rs_label.getString("labelType"));
            }
            
            // If we have an old symbol, we need to create a custom type.
            
            if (!rs_label.getString("_Label_Status_key").equals("1")) {
                
                builder.setDataType(builder.getDataType()+"O");
            }
            
            builder.setData(rs_label.getString("label"));
            builder.setDb_key(rs_label.getString("_Marker_key"));
            builder.setUnique_key(rs_label.getString("_Label_key")
                    + IndexConstants.MARKER_TYPE_NAME);
            displayType = InitCap.initCap(rs_label.getString("labelTypeName"));
            if (displayType.equals("Current Symbol")) {
                displayType = "Symbol";
            }
            builder.setDisplay_type(displayType);
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_label.next();
        }

        // Clean up

        rs_label.close();

        log.info("Done Labels!");
    }
}

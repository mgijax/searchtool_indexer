package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.MarkerExactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;
/**
 * This class is responsible for gathering up any information that we might 
 * need for the markerExact index. This currently consists of non symbol
 * nomenclature.
 * 
 * This information is then used to populate the markerExact index.
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

public class MarkerExactGatherer extends DatabaseGatherer {

    // Class Variables

    private MarkerExactLuceneDocBuilder builder =
        new MarkerExactLuceneDocBuilder();

    /**
     * Create a new instance of the MarkerExactGatherer.
     * 
     * @param config
     */

    public MarkerExactGatherer(IndexCfg config) {

        super(config);
    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create the MarkerExact documents.
     */

    public void runLocal() throws Exception {
            doLabels();
            doAlleleSynonym();
    }

    /**
     * Grab all non symbol labels associated with markers (Alleles and 
     * orthologs included)
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doLabels() throws SQLException, InterruptedException {

        // Grav marker key, label, label type and the type name for all
        // marker, alleles and orthologs, but not the symbols.  
        // Also only do this for the mouse related items, where the marker
        // has not been withdrawn.

        String GENE_LABEL_EXACT = "select ml._Marker_key, "
                + "ml.label, ml.labelType,  ml.labelTypeName, "
                + "ml._OrthologOrganism_key, "
                + "ml._Label_Status_key, ml._Label_key" 
                + " from MRK_Label ml, MRK_Marker m"
                + " where  ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2"
                + "and ml.labelType not in ('MS', 'AS', 'OS')";

        // Gather the data

        ResultSet rs_label = executor.executeMGD(GENE_LABEL_EXACT);
        rs_label.next();

        log.info("Time taken to gather label's result set: "
                + executor.getTiming());

        // Parse it

        String displayType = "";
        String dataType ="";
        while (!rs_label.isAfterLast()) {

            dataType = rs_label.getString("labelType");
            
            if (dataType.equals(IndexConstants.MARKER_SYNOYNM) &&
                    rs_label.getString("_OrthologOrganism_key") != null ) {
                builder.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
            }
            else {
                builder.setDataType(dataType);
            }
                
            if (!rs_label.getString("_Label_Status_key").equals("1")) {
                
                // If we have an old bit of nomen, we need to create a 
                // custom type.
                
                builder.setDataType(builder.getDataType()+"O");
            }
            
            builder.setData(rs_label.getString("label"));
            builder.setDb_key(rs_label.getString("_Marker_key"));
            builder.setUnique_key(rs_label.getString("_Label_key") 
                    + IndexConstants.MARKER_TYPE_NAME);
            displayType = InitCap.initCap(rs_label.getString("labelTypeName"));
            
            // A manual adjustment of the display type for a special case.
            
            if (displayType.equals("Current Name")) {
                displayType = "Name";
            }
            
            builder.setDisplay_type(displayType);
            
            // Add the document to the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_label.next();
        }

        // Clean up

        rs_label.close();

        log.info("Done Labels!");
        
    }

    /**
     * Gather the Allele Synonyms, since they are in a different table.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
        
        private void doAlleleSynonym() 
            throws SQLException, InterruptedException {
        
            // Since Allele synonyms as in thier own table, we need to directly
            // relate them to markers ourselves. 
            // Please note that this is currently using the wrong table
            // as described in TR 9501.  Changing this table over to 
            // ALL_Allele should fix this issue.  
        
            String ALLELE_SYNONYM_KEY = "select distinct gag._Marker_key, "
                + "al.label, al.labelType, al.labelTypeName"
                + " from all_label al, GXD_AlleleGenotype gag"
                + " where al.labelType = 'AY' and al._Allele_key = "
                + "gag._Allele_key and al._Label_Status_key != 0";
        
            // Gather the data
        
            ResultSet rs = executor.executeMGD(ALLELE_SYNONYM_KEY);
            rs.next();
        
            log.info("Time taken gather allele synonym result set "
                    + executor.getTiming());
        
            // Parse it 
        
            while (!rs.isAfterLast()) {
                
                builder.setData(rs.getString("label"));
                builder.setDb_key(rs.getString("_Marker_key")); 
                builder.setDataType(rs.getString("labelType"));
                
                // Since this is the Allele Synonym Section, set its type.
                
                builder.setDisplay_type("Allele Synonym");
                builder.setUnique_key(rs.getString("_Marker_key")
                        +rs.getString("label") 
                        + IndexConstants.MARKER_TYPE_NAME);
                
                // Add the document to the stack
                
                documentStore.push(builder.getDocument());
                builder.clear();
                rs.next();
            }
        
            // Clean up
        
            rs.close();
            log.info("Done Marker Labels!");
        
        }
}

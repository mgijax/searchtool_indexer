package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerExactLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we might 
 * need for the markerExact index. This currently includes nomenclature 
 * and various accession ID's that have been directly related to markers.
 * 
 * @author mhall
 * 
 * @has A hashmap used to translate from shortened codes ("AN") to English 
 * words ("Allele Name")
 * 
 * A single instance of the MarkerExactLuceneDocBuilder which is used 
 * throughout to create its needed Lucene documents
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts 
 * parsing through its result set. For each record, we generate a Lucene 
 * document, and place it on the shared stack.
 * 
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class MarkerExactGatherer extends AbstractGatherer {

    // Class Variables

    private MarkerExactLuceneDocBuilder markerExact =
        new MarkerExactLuceneDocBuilder();

    private Date                        writeStart;
    private Date                        writeEnd;
    
    private Logger log = Logger.getLogger(MarkerExactGatherer.class.getName());

    /**
     * Create a new instance of the MarkerExactGatherer, and populate its
     * translation hashmaps.
     * 
     * @param config
     */

    public MarkerExactGatherer(IndexCfg config) {

        super(config);
    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create a MarkerExact document.
     */

    public void run() {
        try {

            doLabels();
            
            doAlleleSynonym();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Grab all non symbol labels associated with markers (Alleles and 
     * orthologs included)
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doLabels() throws SQLException, InterruptedException {

        // SQL For this Subsection

        String GENE_LABEL_EXACT = "select ml._Marker_key, "
                + "ml.label, ml.labelType,  ml.labelTypeName, "
                + "ml._OrthologOrganism_key, "
                + "ml._Label_Status_key, ml._Label_key" + " from MRK_Label ml,"
                + " MRK_Marker m"
                + " where  ml._Organism_key = 1 and ml._Marker_key = "
                + "m._Marker_key and m._Marker_Status_key !=2"
                + "and ml.labelType not in ('MS', 'AS', 'OS')";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_label = execute(GENE_LABEL_EXACT);
        rs_label.next();

        writeEnd = new Date();

        log.info("Time taken to gather label's result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        String displayType = "";
        String dataType ="";
        while (!rs_label.isAfterLast()) {

            dataType = rs_label.getString("labelType");
            
            if (dataType.equals(IndexConstants.MARKER_SYNOYNM) &&
                    rs_label.getString("_OrthologOrganism_key") != null ) {
                markerExact.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
            }
            else {
                markerExact.setDataType(dataType);
            }
                
            if (!rs_label.getString("_Label_Status_key").equals("1")) {
                
                // If we have an old bit of nomen, we need to create a 
                // custom type.
                
                markerExact.setDataType(markerExact.getDataType()+"O");
            }
            
            markerExact.setData(rs_label.getString("label"));
            markerExact.setDb_key(rs_label.getString("_Marker_key"));
            markerExact.setUnique_key(rs_label.getString("_Label_key") 
                    + IndexConstants.MARKER_TYPE_NAME);
            displayType = initCap(rs_label.getString("labelTypeName"));
            
            if (displayType.equals("Current Name")) {
                displayType = "Name";
            }
            
            markerExact.setDisplay_type(displayType);
            sis.push(markerExact.getDocument());
            markerExact.clear();
            rs_label.next();
        }

        // Clean up

        rs_label.close();

        log.info("Done Labels!");
        
    }

    /**
         * Gather the marker labels.
         * @throws SQLException
         * @throws InterruptedException
         */
        
        private void doAlleleSynonym() 
            throws SQLException, InterruptedException {
        
            // SQL for this Subsection
        
            String ALLELE_SYNONYM_KEY = "select distinct gag._Marker_key, "
                + "al.label, al.labelType, al.labelTypeName"
                + " from all_label al, GXD_AlleleGenotype gag"
                + " where al.labelType = 'AY' and al._Allele_key = "
                + "gag._Allele_key and al._Label_Status_key != 0";
        
            // Gather the data
        
            writeStart = new Date();
        
            ResultSet rs = execute(ALLELE_SYNONYM_KEY);
            rs.next();
        
            writeEnd = new Date();
        
            log.info("Time taken gather allele synonym result set "
                    + (writeEnd.getTime() - writeStart.getTime()));
        
            // Parse it 
        
            while (!rs.isAfterLast()) {
                
                markerExact.setData(rs.getString("label"));
                markerExact.setDb_key(rs.getString("_Marker_key")); 
                markerExact.setDataType(rs.getString("labelType"));
                markerExact.setDisplay_type("Allele Synonym");
                markerExact.setUnique_key(rs.getString("_Marker_key")
                        +rs.getString("label") 
                        + IndexConstants.MARKER_TYPE_NAME);
                
                sis.push(markerExact.getDocument());
                markerExact.clear();
                rs.next();
            }
        
            // Clean up
        
            rs.close();
            log.info("Done Marker Labels!");
        
        }
}

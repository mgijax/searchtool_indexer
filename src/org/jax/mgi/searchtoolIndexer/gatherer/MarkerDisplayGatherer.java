package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.MarkerDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The marker display gatherer is used to collect anything we might need to
 * display for a given marker. This currently includes its symbol, name,
 * chromosomal location and its marker type.
 * 
 * This information is then used to populate the markerDisplay index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Gathers information, and creates Lucene documents for the marker
 * display type, it then populates the shared document stack with these
 * documents, and sets gathering complete to true when its done.
 * 
 */

public class MarkerDisplayGatherer extends DatabaseGatherer {

    // Create our ONLY doc builder.

    private MarkerDisplayLuceneDocBuilder builder =
        new MarkerDisplayLuceneDocBuilder();
    
    /**
     * Create a new MarkerDisplayGatherer, whose environment is setup via its
     * superclass.
     * 
     * @param config
     */

    public MarkerDisplayGatherer(IndexCfg config) {
        super(config);
    }

    /**
     * Start gathering the MarkerDisplay data. This encapsulates the algorithm
     * that knows what types of data are needed for this object.
     */

    public void runLocal() throws Exception {
            doMarkerDisplay();
    }

    /**
     * Grab the Marker Display Information.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doMarkerDisplay() throws SQLException, InterruptedException {

        log.info("Gathering Display Information for Markers");
        
        // Pull in all the marker related information including the following:
        // marker name, marker label, marker symbol, chromosome and accession 
        // id

        String MARKER_DISPLAY_KEY = "select distinct m1._Marker_key, m1.label"
                + " as markername, m2.Label as markersymbol,"
                + " mt.name as markertype, mlc.chromosome, a.accID,"
                + " mlc.startCoordinate, mlc.endCoordinate, mlc.strand"
                + " from MRK_label m1, mrk_marker mrk, mrk_label m2,"
                + " mrk_location_cache mlc, MRK_Types mt, acc_accession a"
                + " where m1._Organism_key = 1 and m1._Label_Status_key = 1"
                + " and m1.labelType = 'MN'  and m1._Marker_key =" 
                + " mrk._Marker_key and mrk._Marker_Status_key != 2 "
                + "and m1._Marker_key = m2._Marker_key and m2.labelType = 'MS'"
                + " and m2._Label_Status_key = 1 and m2._Organism_key = 1"
                + " and m1._Marker_key = mlc._Marker_key"
                + " and mrk._Marker_Type_key = mt._Marker_Type_key "
                + "and m1._Marker_key = a._Object_key and a._LogicalDB_key = 1"
                + " and a.preferred = 1 and a._MGIType_key = 2";

        // Grab the result set

        ResultSet rs = executor.executeMGD(MARKER_DISPLAY_KEY);

        log.info("Time taken gather Marker Display result set: "
                + executor.getTiming());

        // Parse the results.

        while (rs.next()) {
            builder.setDb_key(rs.getString("_Marker_key"));
            builder.setName(rs.getString("markername"));
            builder.setSymbol(rs.getString("markersymbol"));
            builder.setMarker_type(rs.getString("markertype"));
            builder.setChr(rs.getString("chromosome"));
            builder.setAcc_id(rs.getString("accID"));
            builder.setStartCoord(rs.getString("startCoordinate"));
            builder.setStopCoord(rs.getString("stopCoordinate"));
            builder.setStrand(rs.getString("strand"));
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();

        }

        // Clean up
        rs.close();
        log.info("Done gather Marker Display!");

    }

}

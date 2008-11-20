package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;

/**
 * The marker display gatherer is used to collect anything we might need to
 * display for a given marker. This currently includes its symbol, name,
 * chromosomal location and its marker type.
 * 
 * @author mhall
 * 
 * @has MarkerDisplayLuceneDocBuilder which is used to encapsulate all of the
 * information for this object, and then generate lucene documents on demand.
 * 
 * @does Gathers information, and creates Lucene documents for the marker
 * display type, it then populates the shared document stack with these
 * documents, and sets gathering complete to true when its done.
 * 
 */

public class MarkerDisplayGatherer extends AbstractGatherer {

    // Class Variables

    private Date   writeStart;
    private Date   writeEnd;

    // Create our ONLY doc builder.

    private MarkerDisplayLuceneDocBuilder markerDisplay =
        new MarkerDisplayLuceneDocBuilder();

    private Logger log = 
        Logger.getLogger(MarkerDisplayGatherer.class.getName());
    
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

    public void run() {
        try {

            doMarkerDisplay();

        } catch (Exception e) {
            log.error(e);
        } finally {

            sis.setComplete();
            cleanup();
        }
    }

    /**
     * Grab the Marker Display Information.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doMarkerDisplay() throws SQLException, InterruptedException {

        log.info("Gathering Display Information for Markers");
        
        // SQL For this Subsection

        String MARKER_DISPLAY_KEY = "select distinct m1._Marker_key, m1.label"
                + " as markername, m2.Label as markersymbol,"
                + " mt.name as markertype, mlc.chromosome, a.accID"
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

        writeStart = new Date();

        ResultSet rs = execute(MARKER_DISPLAY_KEY);

        writeEnd = new Date();

        log.info("Time taken gather Marker Display result set: "
                + (writeEnd.getTime() - writeStart.getTime()));

        // Parse the results.

        while (rs.next()) {
            markerDisplay.setDb_key(rs.getString("_Marker_key"));
            markerDisplay.setName(rs.getString("markername"));
            markerDisplay.setSymbol(rs.getString("markersymbol"));
            markerDisplay.setMarker_type(rs.getString("markertype"));
            markerDisplay.setChr(rs.getString("chromosome"));
            markerDisplay.setAcc_id(rs.getString("accID"));
            sis.push(markerDisplay.getDocument());
            markerDisplay.clear();

        }

        // Clean up
        rs.close();
        log.info("Done gather Marker Display!");

    }

}

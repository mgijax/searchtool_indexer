package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureDisplayLuceneDocBuilder;
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

public class GenomeFeatureDisplayGatherer extends DatabaseGatherer {

    // Create our ONLY doc builder.

    private GenomeFeatureDisplayLuceneDocBuilder builder =
        new GenomeFeatureDisplayLuceneDocBuilder();

    /**
     * Create a new MarkerDisplayGatherer, whose environment is setup via its
     * superclass.
     *
     * @param config
     */

    public GenomeFeatureDisplayGatherer(IndexCfg config) {
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

        String startCoord;
        String stopCoord;
        String offset;
        String cytoOffset;


        // Pull in all the marker related information including the following:
        // marker name, marker label, marker symbol, chromosome and accession
        // id

        String MARKER_DISPLAY_KEY = "select distinct m1._Marker_key, m1.label"
                + " as markername, m2.Label as markersymbol,"
                + " mt.name as markertype, mlc.chromosome, a.accID,"
                + " convert(varchar(50), mlc.startCoordinate) as startCoord, "
                + " convert(varchar(50), mlc.endCoordinate) as endCoord, "
                + " mlc.strand, mlc.offset, mlc.cytogeneticOffset "
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

            // standard entries
            builder.setDb_key(rs.getString("_Marker_key"));
            builder.setName(rs.getString("markername"));
            builder.setSymbol(rs.getString("markersymbol"));
            builder.setMarker_type(rs.getString("markertype"));
            builder.setChr(rs.getString("chromosome"));
            builder.setAcc_id(rs.getString("accID"));
            builder.setStrand(rs.getString("strand"));

            // derive display values for location
            startCoord  = rs.getString("startCoord");
            stopCoord   = rs.getString("endCoord");
            offset      = rs.getString("offset");
            cytoOffset  = rs.getString("cytogeneticOffset");

            if (startCoord != null && stopCoord != null) { // use coords
              //trim off trailing ".0" and set display value
              startCoord = startCoord.substring(0, startCoord.length() -2);
              stopCoord = stopCoord.substring(0, stopCoord.length() -2);
              builder.setLocDisplay(startCoord + "-" + stopCoord);
            }
            else if (offset != null){
              if (!offset.equals("-999.0")) { // not undetermined offset
                if (offset.equals("-1.0") && cytoOffset == null) {
                  builder.setLocDisplay("Syntenic");
                }
                else if (offset.equals("-1.0") && cytoOffset != null) {
                  //use cyto offset in this case
                  builder.setLocDisplay("cytoband " + cytoOffset);
                }
                else {
                  builder.setLocDisplay(offset + " cM");
                }
              }
            }
            else {
              builder.setLocDisplay("cytoband " + cytoOffset);
            }




            // Place the document on the stack
            documentStore.push(builder.getDocument());
            builder.clear();

        }

        // Clean up
        rs.close();
        log.info("Done gather Marker Display!");

    }

}

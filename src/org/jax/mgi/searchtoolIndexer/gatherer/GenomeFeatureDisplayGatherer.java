package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

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
            doAlleleDisplay();
    }

    /** 
     * Create a hashmap of the allele locations
     * 
     * @return HashMap containing the values;
     */
    
    private HashMap <String, Location> doAlleleLocations() throws SQLException, InterruptedException {
        
        // Intialize the hashmap
        HashMap <String, Location> results = new HashMap <String, Location> ();
        
        log.info("Gathering Allele Location Information");        
        
        String ALL_LOC_KEY = "select a._Allele_key, mlc.chromosome, " +
        		    " convert(varchar(50), mlc.startCoordinate) as startCoordinate, " +
        		    " convert(varchar(50), mlc.endCoordinate) as endCoordinate, mlc.strand," +
        		    " 'MARKER' as source" + 
                    " from all_allele a, MRK_Location_Cache mlc" + 
                    " where a._Marker_key = mlc._Marker_key" + 
                    " union" + 
                    " select a._Allele_key, scc.chromosome, " +
                    " convert(varchar(50), scc.startCoordinate) as startCoordinate," +
                    " convert(varchar(50), scc.endCoordinate) as endCoordinate," +
                    " scc.strand, 'SEQUENCE'" + 
                    " from all_allele a, SEQ_Allele_Assoc saa, SEQ_Coord_Cache scc" + 
                    " where a._Allele_key = saa._Allele_key" + 
                    " and saa._Sequence_key = scc._Sequence_key" + 
                    " and a.isMixed != 1" + 
                    " and saa._Qualifier_key = 3983018" + 
                    " order by a._Allele_key";            
        
        ResultSet rs = executor.executeMGD(ALL_LOC_KEY);

        log.info("Time taken gather Allele Location result set: "
                + executor.getTiming());
        
        while (rs.next()) {
        
            String allele_key = rs.getString("_Allele_key");
            String chr = rs.getString("chromosome");
            String startCoord = rs.getString("startCoordinate");
            String endCoord = rs.getString("endCoordinate");
            String strand = rs.getString("strand");
            String source = rs.getString("source");
            
            if (results.containsKey(allele_key)) {
                Location current = results.get(allele_key);
                if (current.getSource().equals("MARKER") && current.getChromosome().equals("UN") && ! chr.equals("UN")) {
                    current.setChromosome(chr);
                    current.setStart_coordinate(startCoord);
                    current.setEnd_coordinate(endCoord);
                    current.setStrand(strand);
                    current.setSource(source);
                    
                    results.put(allele_key, current);
                }
            }
            else {
                Location current = new Location();
                
                current.setChromosome(chr);
                current.setStart_coordinate(startCoord);
                current.setEnd_coordinate(endCoord);
                current.setStrand(strand);
                current.setSource(source);
                
                results.put(allele_key, current);
            }
            
        }
        
        rs.close();
        
        log.info("Done gathering the Allele Location information.");
        
        return results;
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
                + " and a.preferred = 1 and a._MGIType_key = 2 " 
                + " and mrk._Marker_Type_key != 12";

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
            builder.setObjectType("MARKER");
            builder.setBatchValue(rs.getString("markerSymbol"));

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

    private void doAlleleDisplay() throws SQLException, InterruptedException {

        log.info("Gathering Display Information for Alleles");

/*        String startCoord;
        String stopCoord;
        String offset;
        String cytoOffset;*/


        // Pull in all the Allele related information including the following:
        // allele name, allele label, allele symbol, chromosome and accession
        // id

        String ALLELE_DISPLAY_KEY = "select aa._Allele_key, aa.name as allele_name, aa.symbol, " + 
                                    "vt.term as alleletype, '' as chromosome, a.accID, " + 
                                    "'' as startCoord, '' as endCoord, '' as strand, '' " + 
                                    "as offset, '' as cytogeneticOffset, m.symbol as marker_symbol, " +
                                    "m.name as marker_name " + 
                                    "from all_allele aa, voc_term vt, acc_accession a, mrk_marker m " +
                                    "where aa._Allele_Type_key = vt._Term_key and aa._Allele_key " + 
                                    "*= a._Object_key and a.private != 1 and a.preferred = 1 " + 
                                    "and a.prefixPart = 'MGI:' and a._LogicalDB_key = 1 and " + 
                                    "a._MGIType_key = 2 and aa.isWildType != 1 " +
                                    "and aa._Marker_key *= m._Marker_key";

        // Grab the result set
        ResultSet rs = executor.executeMGD(ALLELE_DISPLAY_KEY);

        log.info("Time taken gather Marker Display result set: "
                + executor.getTiming());

        HashMap <String, Location> locationMap = doAlleleLocations();
        
        // Parse the results.
        while (rs.next()) {

            // standard entries
            String allele_key = rs.getString("_Allele_key");
            
            builder.setDb_key(allele_key);

            if (rs.getString("marker_name") != null && (! rs.getString("marker_name").equals(rs.getString("allele_name")))) {
                builder.setName(rs.getString("marker_name") + "; " + rs.getString("allele_name"));
            }
            else {
                builder.setName(rs.getString("allele_name"));
            }
            
            builder.setSymbol(rs.getString("symbol"));
            String all_type = rs.getString("alleletype");
            
            if (all_type.equals("Not Specified") || all_type.equals("Not Applicable")) {
                all_type = "";
            }
            else if (all_type.startsWith("Targeted")) {
                all_type = "Targeted allele";
            }
            else if (all_type.startsWith("Transgenic")) {
                all_type = "Transgene";
            }
            else if (all_type.startsWith("Chemically")) {
                all_type = "Chemically induced allele";
            }
            else {
                all_type = all_type + " allele";
            }
            
            builder.setMarker_type(all_type);
            builder.setChr(rs.getString("chromosome"));
            builder.setAcc_id(rs.getString("accID"));
            builder.setStrand(rs.getString("strand"));
            builder.setObjectType("ALLELE");
            if (! all_type.equals("Transgene")) {
                builder.setBatchValue(rs.getString("marker_symbol"));
            }

            if (locationMap.containsKey(allele_key)) {

                String start = locationMap.get(allele_key).getStart_coordinate();
                String end = locationMap.get(allele_key).getEnd_coordinate();
                
                String location = "";
                
                if (start != null) {
                    location = start.replace(".0", "") 
                        + "-" + end.replace(".0", "");
                }
                
                builder.setStrand(locationMap.get(allele_key).getStrand());
                builder.setChr(locationMap.get(allele_key).getChromosome());
                builder.setLocDisplay(location);
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

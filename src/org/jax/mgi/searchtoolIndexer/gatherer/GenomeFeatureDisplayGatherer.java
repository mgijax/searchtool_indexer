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
 *       display type, it then populates the shared document stack with these
 *       documents, and sets gathering complete to true when its done.
 * 
 */

public class GenomeFeatureDisplayGatherer extends DatabaseGatherer {

	// Create our ONLY doc builder.

	private GenomeFeatureDisplayLuceneDocBuilder builder = new GenomeFeatureDisplayLuceneDocBuilder();

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

	private HashMap<String, Location> doAlleleLocations() throws SQLException,
			InterruptedException {

		// Intialize the hashmap
		HashMap<String, Location> results = new HashMap<String, Location>();

		log.info("Gathering Allele Location Information");

		String ALL_LOC_KEY = "select a._Allele_key, mlc.genomicChromosome as chromosome, "
				+ " to_char(mlc.startCoordinate, '999999999999') as startCoordinate, "
				+ " to_char(mlc.endCoordinate, '999999999999') as endCoordinate, mlc.strand,"
				+ " 'MARKER' as source"
				+ " from all_allele a, MRK_Location_Cache mlc"
				+ " where a._Marker_key = mlc._Marker_key"
				+ " and mlc.genomicChromosome is not null"
				+ " union"
				+ " select a._Allele_key, mlc.chromosome,"
				+ " to_char(mlc.startCoordinate, '999999999999') as startCoordinate,"
				+ " to_char(mlc.endCoordinate, '999999999999') as endCoordinate, mlc.strand,"
				+ " 'MARKER' as source"
				+ " from all_allele a, MRK_Location_Cache mlc"
				+ " where a._Marker_key = mlc._Marker_key"
				+ " and mlc.genomicChromosome is null"
				+ " union"
				+ " select a._Allele_key, scc.chromosome, "
				+ " to_char(scc.startCoordinate, '999999999999') as startCoordinate,"
				+ " to_char(scc.endCoordinate, '999999999999') as endCoordinate,"
				+ " scc.strand, 'SEQUENCE'"
				+ " from all_allele a, SEQ_Allele_Assoc saa, SEQ_Coord_Cache scc"
				+ " where a._Allele_key = saa._Allele_key"
				+ " and saa._Sequence_key = scc._Sequence_key"
				+ " and a.isMixed != 1"
				+ " and saa._Qualifier_key = 3983018"
				+ " order by 1";

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
				if (current.getSource().equals("MARKER")
						&& current.getChromosome().equals("UN")
						&& !chr.equals("UN")) {
					current.setChromosome(chr);
					current.setStart_coordinate(startCoord);
					current.setEnd_coordinate(endCoord);
					current.setStrand(strand);
					current.setSource(source);

					results.put(allele_key, current);
				}
			} else {
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

		String MARKER_DISPLAY_KEY = "select distinct mmv._Marker_key, mmv.name, mmv.symbol, "
				+ "  mmc.directTerms, a.accID, "
				+ "  to_char(mlc.startCoordinate, '999999999999') as startCoord, "
				+ "  to_char(mlc.endCoordinate, '999999999999') as endCoord, "
				+ "  mlc.strand, mlc.cmOffset, mlc.cytogeneticOffset, "
				+ "  mlc.genomicChromosome as chromosome "
				+ "from MRK_Marker_View mmv, "
				+ "  ACC_Accession a, "
				+ "  MRK_Location_Cache mlc, "
				+ "  MRK_MCV_Cache mmc "
				+ "where mmv._Marker_Status_key != 2 and mmv._Organism_key = 1 "
				+ "  and mmv._Marker_key = a._Object_key "
				+ "  and a.prefixPart = 'MGI:' "
				+ "  and a.private != 1 and a.preferred = 1 "
				+ "  and a._MGIType_key = 2 "
				+ "  and mmv._Marker_key = mlc._Marker_key "
				+ "  and mlc.genomicChromosome is not null "
				+ "  and mmv._Marker_Type_key != 12 "
				+ "  and mmv._Marker_key = mmc._Marker_key "
				+ "UNION "
				+ "select distinct mmv._Marker_key, mmv.name, mmv.symbol, "
				+ "  mmc.directTerms, a.accID, "
				+ "  to_char(mlc.startCoordinate, '999999999999') as startCoord, "
				+ "  to_char(mlc.endCoordinate, '999999999999') as endCoord, "
				+ "  mlc.strand, mlc.cmOffset, mlc.cytogeneticOffset, "
				+ "  mlc.chromosome "
				+ "from MRK_Marker_View mmv, "
				+ "  ACC_Accession a, "
				+ "  MRK_Location_Cache mlc, "
				+ "  MRK_MCV_Cache mmc "
				+ "where mmv._Marker_Status_key != 2 and mmv._Organism_key = 1 "
				+ "  and mmv._Marker_key = a._Object_key "
				+ "  and a.prefixPart = 'MGI:' "
				+ "  and a.private != 1 and a.preferred = 1 "
				+ "  and a._MGIType_key = 2 "
				+ "  and mmv._Marker_key = mlc._Marker_key "
				+ "  and mlc.genomicChromosome is null "
				+ "  and mmv._Marker_Type_key != 12 "
				+ "  and mmv._Marker_key = mmc._Marker_key";

		// Grab the result set
		ResultSet rs = executor.executeMGD(MARKER_DISPLAY_KEY);

		log.info("Time taken gather Marker Display result set: " + executor.getTiming());

		// Parse the results.
		while (rs.next()) {

			// standard entries
			builder.setDb_key(rs.getString("_Marker_key"));
			builder.setName(rs.getString("name"));
			builder.setSymbol(rs.getString("symbol"));
			builder.setMarker_type(rs.getString("directTerms"));
			builder.setChr(rs.getString("chromosome"));
			builder.setAcc_id(rs.getString("accID"));
			builder.setStrand(rs.getString("strand"));
			builder.setObjectType("MARKER");
			builder.setBatchValue(rs.getString("symbol"));

			// derive display values for location
			startCoord = rs.getString("startCoord");
			stopCoord = rs.getString("endCoord");
			offset = rs.getString("cmOffset");
			cytoOffset = rs.getString("cytogeneticOffset");

			if (startCoord != null && stopCoord != null) { // use coords
				// trim off trailing ".0" and set display value
				if (startCoord.endsWith(".0")) {
					startCoord = startCoord.substring(0, startCoord.length() - 2);
				}
				if (stopCoord.endsWith(".0")) {
					stopCoord = stopCoord.substring(0, stopCoord.length() - 2);
				}
				builder.setLocDisplay(startCoord + "-" + stopCoord);
			} else if (offset != null) {

				// trim off the decimal portion of the string
				offset = offset.replaceAll("\\.[0-9]+", "");

				if (!offset.equals("-999")) { // not undetermined offset
					if (offset.equals("-1") && cytoOffset == null) {
						builder.setLocDisplay("Syntenic");
					} else if (offset.equals("-1") && cytoOffset != null) {
						// use cyto offset in this case
						builder.setLocDisplay("cytoband " + cytoOffset);
					} else {
						builder.setLocDisplay(offset + " cM");
					}
				}
			} else {
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

		/*
		 * String startCoord; String stopCoord; String offset; String
		 * cytoOffset;
		 */

		// Pull in all the Allele related information including the following:
		// allele name, allele label, allele symbol, chromosome and accession
		// id

		String ALLELE_DISPLAY_KEY = "select aa._Allele_key, "
				+ "  aa.name as allele_name, " + "  aa.symbol, "
				+ "  vt.term as alleletype, " + "  '' as chromosome, "
				+ "   a.accID, " + "  '' as startCoord, "
				+ "  '' as endCoord, " + "  '' as strand, "
				+ "  '' as cmOffset, " + "  '' as cytogeneticOffset, "
				+ "  m.symbol as marker_symbol, " + "  m.name as marker_name "
				+ "from all_allele aa " + "inner join voc_term vt on ("
				+ "  aa._Allele_Type_key = vt._Term_key) "
				+ "left outer join mrk_marker m on ("
				+ "  aa._Marker_key = m._Marker_key)"
				+ "left outer join acc_accession a on ("
				+ "  aa._Allele_key = a._Object_key " + "  and a.private != 1 "
				+ "  and a.preferred = 1 " + "  and a.prefixPart = 'MGI:' "
				+ "  and a._LogicalDB_key = 1 " + "  and a._MGIType_key = 2) "
				+ "where aa.isWildType != 1";

		// Grab the result set
		ResultSet rs = executor.executeMGD(ALLELE_DISPLAY_KEY);

		log.info("Time taken gather Marker Display result set: " + executor.getTiming());

		HashMap<String, Location> locationMap = doAlleleLocations();

		// Parse the results.
		while (rs.next()) {

			// standard entries
			String allele_key = rs.getString("_Allele_key");

			builder.setDb_key(allele_key);

			if (rs.getString("marker_name") != null && (!rs.getString("marker_name").equals(rs.getString("allele_name")))) {
				builder.setName(rs.getString("marker_name") + "; " + rs.getString("allele_name"));
			} else {
				builder.setName(rs.getString("allele_name"));
			}

			builder.setSymbol(rs.getString("symbol"));
			String all_type = rs.getString("alleletype");

			if (all_type.equals("Not Specified") || all_type.equals("Not Applicable")) {
				all_type = "";
			} else if (all_type.startsWith("Targeted")) {
				all_type = "Targeted allele";
			} else if (all_type.startsWith("Transgenic")) {
				all_type = "Transgene";
			} else if (all_type.startsWith("Chemically")) {
				all_type = "Chemically induced allele";
			} else {
				all_type = all_type + " allele";
			}

			builder.setMarker_type(all_type);
			builder.setChr(rs.getString("chromosome"));
			builder.setAcc_id(rs.getString("accID"));
			builder.setStrand(rs.getString("strand"));
			builder.setObjectType("ALLELE");
			if (!all_type.equals("Transgene")) {
				builder.setBatchValue(rs.getString("marker_symbol"));
			}

			if (locationMap.containsKey(allele_key)) {

				String start = locationMap.get(allele_key).getStart_coordinate();
				String end = locationMap.get(allele_key).getEnd_coordinate();

				String location = "";

				if (start != null) {
					location = start.replace(".0", "") + "-" + end.replace(".0", "");
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

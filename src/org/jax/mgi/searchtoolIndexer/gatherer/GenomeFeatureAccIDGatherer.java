package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureAccIDLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.searchtoolIndexer.util.ProviderHashMap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;
import java.util.HashMap;

/**
 * This is responsible for gathering up and information we might need in the
 * MarkerAccID index.
 * 
 * @author mhall
 * 
 * @has An instance of IndexCfg, which is used to setup this object.
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 *       Each component basically makes a call to the database and then starts
 *       parsing through its result set. For each record, we generate a Lucene
 *       document, and place it on the shared stack.
 * 
 *       After all of the components are finished, we notify the stack that
 *       gathering is complete, clean up our jdbc connections and exit.
 */

public class GenomeFeatureAccIDGatherer extends DatabaseGatherer {

	// Class Variables

	private GenomeFeatureAccIDLuceneDocBuilder builder = new GenomeFeatureAccIDLuceneDocBuilder();

	private ProviderHashMap phmg;

	/**
	 * Create a new instance of the MarkerExactGatherer, and populate its
	 * translation hash maps.
	 * 
	 * @param config
	 */

	public GenomeFeatureAccIDGatherer(IndexCfg config) {

		super(config);

		phmg = new ProviderHashMap(config);

	}

	/**
	 * This method encapsulates the algorithm used for gathering the data needed
	 * to create the MarkerExact documents.
	 * 
	 */

	public void runLocal() throws Exception {
		doMarkerAccession();
		doAlleleAccession();
		doOrthologAccession();
		doAllelesByESCellLines();
		doAllelesBySequence();
		doAllelesByESCellLineNames();
		doAllelesByMarkerTransgene();
	}

	/**
	 * Grab accession ID's directly associated with markers.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doMarkerAccession() throws SQLException, InterruptedException {

		// SQL for this Subsection

		log.info("Gathering Accession ID's for Markers");

		// Select all marker related accession id's, where the id is
		// not private, where its for the mouse, and the marker has not
		// been withdrawn. Order to prefer lower logical databases.
		// Middle of union brings in all non-private seq IDs for
		// sequences associated to markers.  Exclude MyGene IDs, though.
		// Bottom of union brings in IDs for strain markers associated
		// with the given canonical marker.

		String GENE_ACC_KEY = "SELECT a._Object_key, a.accID, "
			+ "    a._LogicalDB_key "
			+ "FROM ACC_Accession a, MRK_Marker m "
			+ "WHERE a.private = 0 "
			+ "    AND a._MGIType_key = 2 "
			+ "    AND a._Object_key = m._Marker_key "
			+ "    AND m._Organism_key = 1"
			+ "    AND m._Marker_Status_key = 1 "
			+ "    AND m._Marker_Type_key != 12 "
			+ "    AND a._LogicalDB_key != 178 "
			+ "UNION "
			+ "SELECT smc._Marker_key, a.accID, a._LogicalDB_key "
			+ "FROM SEQ_Marker_Cache smc, "
			+ "    MRK_Marker m, ACC_Accession a "
			+ "WHERE smc._Marker_key = m._Marker_key "
			+ "    AND m._Organism_key = 1 "
			+ "    AND m._Marker_Status_key = 1 "
			+ "    AND m._Marker_Type_key != 12 "
			+ "    AND a._MGIType_key = 19 "
			+ "    AND a._Object_key = smc._Sequence_key "
			+ "    AND a.private = 0 "
			+ "UNION "
			+ "SELECT msm._Marker_key, a_seq.accID, a_seq._LogicalDB_key "
			+ "FROM MRK_StrainMarker msm, ACC_Accession a, ACC_Accession a_seq "
			+ "WHERE msm._StrainMarker_key = a._Object_key "
			+ "    AND a._MGIType_key = 44 "
			+ "    AND a.accID = a_seq.accID "
			+ "    AND a_seq._MGIType_key = 19 "
			+ "    AND msm._Marker_key is not null "
			+ "order by _Object_key, accID, _LogicalDB_key";

		// Gather the data

		ResultSet rs_acc = executor.executeMGD(GENE_ACC_KEY);

		String provider = "";

		log.info("Time taken to gather marker's accession id result set: " + executor.getTiming());

		// keys are Entrez Gene IDs for this particular marker, so we don't
		// repeat the same ID for NCBI Gene Model (we prefer to show Entrez
		// Gene)
		HashMap egIDs = new HashMap();

		String logicalDB = null;
		String accID = null;
		String objectKey = null;
		String lastObjectKey = "none";

		boolean skipThisOne = false;

		// Parse it

		while (rs_acc.next()) {

			skipThisOne = false;

			objectKey = rs_acc.getString("_Object_key");
			accID = rs_acc.getString("accID");
			logicalDB = rs_acc.getString("_LogicalDB_key");

			// if this is a new object, reset our cache of Entrez Gene IDs
			if (!lastObjectKey.equals(objectKey)) {
				egIDs.clear();
				lastObjectKey = objectKey;
			}

			if ("55".equals(logicalDB)) {
				// Entrez Gene ID
				egIDs.put(accID, "55");

			} else if ("59".equals(logicalDB)) {

				// NCBI Gene Model ID
				if (egIDs.get(accID) != null) {
					skipThisOne = true;
				}
			}

			builder.setData(accID);
			builder.setDb_key(objectKey);
			builder.setDataType(IndexConstants.ACCESSION_ID);
			builder.setDisplay_type("ID");
			builder.setObject_type("MARKER");
			provider = phmg.get(logicalDB);

			// Set the provider, blanking it out if needed.

			if (!provider.equals("")) {
				builder.setProvider("(" + provider + ")");
			} else {
				builder.setProvider(provider);
			}

			// Place the document on the stack (unless we need to skip it)

			if (!skipThisOne) {
				documentStore.push(builder.getDocument());
			}

			builder.clear();
		}

		// Clean up

		rs_acc.close();
		log.info("Done Accession ID's for Markers!");
	}

	/**
	 * Grab allele accession ID's that have been associated with markers.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAlleleAccession() throws SQLException, InterruptedException {

		log.info("Gathering Accession ID's for Alleles");

		// Gather up all allele accession ID's and the markers they are
		// related to, as long as the marker hasn't been withdrawn.

		String ALLELE_TO_MARKER_EXACT = "select al._allele_key,  ac.accID"
				+ " from ALL_Allele al, ACC_Accession ac"
				+ " where al._Allele_key = ac._Object_key and"
				+ " ac._MGIType_key = 11 and ac.private != 1 "
				+ " and al.isWildType != 1";

		// Gather the data

		ResultSet rs_all_acc = executor.executeMGD(ALLELE_TO_MARKER_EXACT);


		log.info("Time taken to gather Allele's Accession ID result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_all_acc.next()) {
			builder.setData(rs_all_acc.getString("accID"));
			builder.setDb_key(rs_all_acc.getString("_allele_key"));
			builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
			builder.setDisplay_type("Allele ID");
			builder.setObject_type("ALLELE");

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
		}

		// Clean up

		rs_all_acc.close();
		log.info("Done Allele Accession ID's");
	}

	/**
	 * Grab Ortholog Accession ID's that have been directly associated to
	 * Markers.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doOrthologAccession() throws SQLException, InterruptedException {

		log.info("Gathering Accession ID's for Orthologs");

		// Collect accession IDs for non-mouse markers that can be related to
		// mouse markers via homology classes.  Lower part of the union brings in
		// the numeric part of OMIM gene IDs (for human markers) as IDs, too.

		String ORTH_TO_MARKER_ACC_ID = "select distinct mouse._Marker_key, "
				+ " aa.accID, aa._LogicalDB_key, o.commonName "
				+ "from VOC_Term source, " + " MRK_Cluster mc, "
				+ " MRK_ClusterMember nonmouse, "
				+ " MRK_ClusterMember mouse, " + " MRK_Marker mm, "
				+ " ACC_Accession aa, " + " MRK_Marker nm, "
				+ " MGI_Organism o " + "where source.abbreviation = 'Alliance Direct' "
				+ " and source._Term_key = mc._ClusterSource_key "
				+ " and mc._Cluster_key = mouse._Cluster_key "
				+ " and mouse._Marker_key = mm._Marker_key "
				+ " and mm._Organism_key = 1 "
				+ " and mm._Marker_Status_key = 1 "
				+ " and mm._Marker_Type_key != 12 "
				+ " and mc._Cluster_key = nonmouse._Cluster_key "
				+ " and nonmouse._Marker_key = nm._Marker_key "
				+ " and nm._Organism_key != 1 "
				+ " and nm._Organism_key = o._Organism_key "
				+ " and nm._Marker_Status_key = 1 "
				+ " and nonmouse._Marker_key = aa._Object_key "
				+ " and aa._LogicalDB_key != 178 "
				+ " and aa._MGIType_key = 2 and aa.private = 0 "
				+ "UNION "
				+ "select distinct mouse._Marker_key, aa.numericPart::text, aa._LogicalDB_key, o.commonName "
				+ "from VOC_Term source, MRK_Cluster mc, MRK_ClusterMember nonmouse, "
				+ " MRK_ClusterMember mouse, MRK_Marker mm, ACC_Accession aa, MRK_Marker nm, MGI_Organism o "
				+ "where source.abbreviation = 'Alliance Direct' "
				+ " and source._Term_key = mc._ClusterSource_key "
				+ " and mc._Cluster_key = mouse._Cluster_key "
				+ " and mouse._Marker_key = mm._Marker_key "
				+ " and mm._Organism_key = 1 "
				+ " and mm._Marker_Status_key = 1 "
				+ " and mm._Marker_Type_key != 12 "
				+ " and mc._Cluster_key = nonmouse._Cluster_key "
				+ " and nonmouse._Marker_key = nm._Marker_key "
				+ " and nm._Organism_key = 2 "
				+ " and nm._Organism_key = o._Organism_key "
				+ " and nm._Marker_Status_key = 1 "
				+ " and nonmouse._Marker_key = aa._Object_key "
				+ " and aa._LogicalDB_key = 15 "
				+ " and aa._MGIType_key = 2 and aa.private = 0";

		// Gather the data

		ResultSet rs_orth_acc = executor.executeMGD(ORTH_TO_MARKER_ACC_ID);


		int i = 0;
		log.info("Time taken to gather Ortholog's Accession ID result set: " + executor.getTiming());

		// Parse it

		while (rs_orth_acc.next()) {

			builder.setData(rs_orth_acc.getString("accID"));
			builder.setDb_key(rs_orth_acc.getString("_Marker_key"));
			builder.setDataType(IndexConstants.ORTH_ACCESSION_ID);
			builder.setDisplay_type("ID");
			builder.setObject_type("MARKER");

			// Another special case for the provider hash map. We must append
			// the organism's name for these cases.

			builder.setProvider("("
					+ phmg.get(rs_orth_acc.getString("_LogicalDB_key")) + " - "
					+ InitCap.initCap(rs_orth_acc.getString("commonName"))
					+ ")");

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			i++;
		}

		// Clean up

		rs_orth_acc.close();
		log.info("Added " + i + " Ortholog Accession ID's!");

	}

	/**
	 * Gather Alleles by thier marker transgene ID's
	 * 
	 * These are then placed into the index as allele objects.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAllelesByMarkerTransgene() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the accession id's for marker transgene ID's that are
		// related to
		// alleles.

		String ALLELE_MARKER_TRANSGENE_SEARCH = "select distinct a.accID, aa._Allele_key, 'ALLELE' as _MGIType_key,"
				+ " a.preferred, a._LogicalDB_key "
				+ "from mrk_marker as m, acc_accession as a, all_allele as aa "
				+ "where m._Marker_Type_key = 12 and "
				+ "m._Marker_key = a._Object_key and a._MGIType_key = 2 "
				+ "and aa._Marker_key = m._Marker_key";

		// Gather the data

		ResultSet rs_transgene = executor.executeMGD(ALLELE_MARKER_TRANSGENE_SEARCH);


		log.info("Time taken gather marker transgene -> alelle data set: " + executor.getTiming());

		// Parse it

		while (rs_transgene.next()) {
			builder.setData(rs_transgene.getString("accID"));
			builder.setDb_key(rs_transgene.getString("_Allele_key"));
			builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
			builder.setDisplay_type("ID");
			builder.setObject_type("ALLELE");

			String provider = phmg
					.get(rs_transgene.getString("_LogicalDB_key"));

			// Set the provider, blanking it out if needed.

			if (!provider.equals("")) {
				builder.setProvider("(" + provider + ")");
			} else {
				builder.setProvider(provider);
			}

			documentStore.push(builder.getDocument());

			builder.clear();
		}

		// Clean up

		log.info("Done creating documents for marker transgenes -> allele");
		rs_transgene.close();
	}

	/**
	 * Gather the ES Cell Line data. Please note, we only gather Accession ID's
	 * for this data type if they have a direct relationship to an allele.
	 * 
	 * These are then placed into the index as allele objects.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAllelesByESCellLines() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the accession id's for es cell that are related to
		// alleles.

		String OTHER_ES_CELL_LINE_SEARCH = "SELECT distinct a._Accession_key,"
				+ " a.accID, aa._Allele_key, 'ALLELE' as _MGIType_key,"
				+ " a.preferred, a._LogicalDB_key"
				+ " FROM ACC_Accession a, all_allele aa, ALL_Allele_Cellline aac"
				+ " where a.private != 1 and a._MGIType_key = 28 and"
				+ " aa._Allele_key = aac._Allele_key and"
				+ " a._Object_key = aac._MutantCellLine_key";

		// Gather the data

		ResultSet rs_escell = executor.executeMGD(OTHER_ES_CELL_LINE_SEARCH);

		log.info("Time taken gather es cell line data set: " + executor.getTiming());

		// Parse it

		while (rs_escell.next()) {
			builder.setData(rs_escell.getString("accID"));
			builder.setDb_key(rs_escell.getString("_Allele_key"));
			builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
			builder.setDisplay_type("Cell Line ID");
			builder.setObject_type("ALLELE");

			String provider = phmg.get(rs_escell.getString("_LogicalDB_key"));

			// Set the provider, blanking it out if needed.

			if (!provider.equals("")) {
				builder.setProvider("(" + provider + ")");
			} else {
				builder.setProvider(provider);
			}

			documentStore.push(builder.getDocument());

			builder.clear();
		}

		// Clean up

		log.info("Done creating documents for es cell lines!");
		rs_escell.close();
	}

	/*    *//**
	 * Gather the allele by sequence data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAllelesBySequence() throws SQLException,
	InterruptedException {

		// SQL for this Subsection

		// gather up the non private accession id's for alleles

		String OTHER_ALL_BY_SEQUENCE_SEARCH = "select distinct a._Accession_key, a.accID,"
				+ " saa._Allele_key, 'ALLELE' as _MGIType_key,"
				+ " a.preferred, a._LogicalDB_key"
				+ " from ACC_Accession a, SEQ_Allele_Assoc saa"
				+ " where a._MGIType_key = 19 and a.private != 1"
				+ " and saa._Sequence_key = a._Object_key and saa._Allele_key is not null";

		ResultSet rs_all = executor.executeMGD(OTHER_ALL_BY_SEQUENCE_SEARCH);

		log.info("Time taken gather allele by sequence data set: " + executor.getTiming());

		while (rs_all.next()) {

			builder.setData(rs_all.getString("accID"));
			builder.setDb_key(rs_all.getString("_Allele_key"));
			builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
			builder.setDisplay_type("ID");
			builder.setObject_type("ALLELE");

			String provider = phmg.get(rs_all.getString("_LogicalDB_key"));

			// Set the provider, blanking it out if needed.

			if (!provider.equals("")) {
				builder.setProvider("(" + provider + ")");
			} else {
				builder.setProvider(provider);
			}

			documentStore.push(builder.getDocument());

			builder.clear();
		}

		// Clean up

		log.info("Done creating documents for alleles!");
		rs_all.close();
	}

	/**
	 * Gather the ES Cell Line data for the ones that have only a name, and no
	 * accession ID.
	 * 
	 * These are then placed into the index as allele objects.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAllelesByESCellLineNames() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the accession id's for es cell that are related to
		// alleles.

		log.info("Beginning ES cell line names for alleles...");
		String OTHER_ES_CELL_LINE_SEARCH = "select aac._Allele_key, ac.cellLine "
				+ "from all_cellline ac, ALL_Allele_CellLine aac "
				+ "where ac.cellLine != 'Not Specified' "
				+ " and ac.cellLine != 'Other (see notes)' "
				+ " and ac.isMutant = 1 "
				+ " and ac._CellLine_key = aac._MutantCellLine_key "
				+ " and not exists (select 1 from acc_accession aa "
				+ "  where aa._Object_key = ac._CellLine_key "
				+ "  and _MGIType_key = 28 "
				+ "  and private != 1)";

		// Gather the data

		ResultSet rs_escell = executor.executeMGD(OTHER_ES_CELL_LINE_SEARCH);

		log.info("Time taken gather es cell line name data set: " + executor.getTiming());

		while (rs_escell.next()) {
			builder.setData(rs_escell.getString("cellLine"));
			builder.setDb_key(rs_escell.getString("_Allele_key"));
			builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
			builder.setDisplay_type("Cell Line");
			builder.setObject_type("ALLELE");

			documentStore.push(builder.getDocument());

			builder.clear();
		}

		log.info("Done creating documents for es cell lines!");
		rs_escell.close();
	}

}

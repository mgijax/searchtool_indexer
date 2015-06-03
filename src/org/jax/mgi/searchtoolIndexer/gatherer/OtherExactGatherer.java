package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.OtherExactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.searchtoolIndexer.util.ProviderHashMap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up all the different sorts of
 * information that we need to perform searches against accession id's. It is
 * however important to note that this is not always direct accid->object
 * relations. We do more complex things like es cell line accession id's ->
 * alleles, and probes -> sequences as well.
 * 
 * This information is then used to populate the otherExact index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started this runs through a group of methods, each of which
 *       are responsible for gathering documents from a different accession id
 *       type.
 * 
 *       Each subprocess basically operates as follows:
 * 
 *       Gather the data for the specific subtype, parse it while creating
 *       lucene documents and adding them to the stack.
 * 
 *       After it completes parsing, it cleans up its result sets, and exits.
 * 
 *       After all of these methods complete, we set gathering complete to true
 *       in the shared document stack and exit.
 * 
 */

public class OtherExactGatherer extends DatabaseGatherer {

	// Class Variables

	private double						total				= 0;
	private double						output_incrementer	= 100000;
	private double						output_threshold	= 100000;

	// The single LuceneDocBuilder for this Object.

	private OtherExactLuceneDocBuilder	builder	= new OtherExactLuceneDocBuilder();

	private ProviderHashMap				phm;

	public OtherExactGatherer(IndexCfg config) {
		super(config);
		phm = new ProviderHashMap(config);
	}

	/**
	 * This is the runLocal method, which is called by the supers run() method.
	 * It encapsulates the work that this specific implementing object needs to
	 * perform in order to get its work done.
	 */

	public void runLocal() throws Exception {

		//Generic Searches
		doAccessionByType(IndexConstants.OTHER_GENOTYPE, "12", true);
		doAccessionByType(IndexConstants.OTHER_REFERENCE, "1", true);
		doAccessionByType(IndexConstants.OTHER_PROBE, "3", true);
		doAccessionByType(IndexConstants.OTHER_ASSAY, "8", false);
		doAccessionByType(IndexConstants.OTHER_ANTIBODY, "6", false);
		doAccessionByType(IndexConstants.OTHER_EXPERIMENT, "4", false);
		doAccessionByType(IndexConstants.OTHER_IMAGE, "9", false);		

		// Custom Searches
		doOrthologs();
		doSequences();
		doSequencesByProbe();
		doAMA();
	}

	public void doAccessionByType(String mgiTypeKey, String mgiTypeKeyId, boolean setProvider) throws SQLException, InterruptedException {

		// If this query does not suit your needs create a custom query.
		String OTHER_GENERIC_SEARCH = "SELECT distinct a._Accession_key, "
				+ "a.accID, a._Object_key, '" + mgiTypeKey
				+ "' as _MGIType_key, a.preferred, a._LogicalDB_key"
				+ " FROM ACC_Accession a"
				+ " where a.private != 1 and a._MGIType_key = " + mgiTypeKeyId;

		ResultSet rs_ref = executor.executeMGD(OTHER_GENERIC_SEARCH);
		rs_ref.next();

		log.info("Time taken gather reference data set: " + executor.getTiming());

		// Parse it

		while (!rs_ref.isAfterLast()) {

			builder.setType(rs_ref.getString("_MGIType_key"));
			builder.setData(rs_ref.getString("accID"));
			builder.setDb_key(rs_ref.getString("_Object_key"));
			builder.setAccessionKey(rs_ref.getString("_Accession_key"));
			builder.setPreferred(rs_ref.getString("preferred"));
			if(setProvider) builder.setProvider(phm.get(rs_ref.getString("_LogicalDB_key")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
			rs_ref.next();
		}

		// Clean up

		log.info("Done creating documents for " + mgiTypeKey + "!");
		rs_ref.close();
	}





	/**
	 * Gather the sequence data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doSequences() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up the non private accession id's for sequences, for mouse
		// only sequences.

		String OTHER_SEQ_SEARCH = "SELECT distinct a._Accession_key, a.accID,"
				+ " a._Object_key, '" + IndexConstants.OTHER_SEQUENCE
				+ "' as _MGIType_key, a.preferred, a._LogicalDB_key"
				+ " FROM ACC_Accession a, SEQ_Sequence s"
				+ " where a.private != 1 and a._MGIType_key = 19 and"
				+ " a._Object_key = s._Sequence_key and"
				+ " s._Organism_key = 1";

		// Gather the data.

		ResultSet rs_seq = executor.executeMGD(OTHER_SEQ_SEARCH);
		rs_seq.next();

		log.info("Time taken gather sequence data set: " + executor.getTiming());

		// Parse it

		while (!rs_seq.isAfterLast()) {

			builder.setType(rs_seq.getString("_MGIType_key"));
			builder.setData(rs_seq.getString("accID"));
			builder.setDb_key(rs_seq.getString("_Object_key"));
			builder.setAccessionKey(rs_seq.getString("_Accession_key"));
			builder.setPreferred(rs_seq.getString("preferred"));
			builder.setProvider(phm.get(rs_seq.getString("_LogicalDB_key")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}

			rs_seq.next();
		}

		// Clean up

		log.info("Done creating documents for sequences!");
		rs_seq.close();
	}

	/**
	 * Gather Sequences by way of Probe Accession ID's. This subsections SQL is
	 * significantly more complex than the others.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doSequencesByProbe()
			throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the accession ids for probes, and then assign them to the
		// sequences that these probes point at for non private accession ids
		// where the organism is a mouse.

		String OTHER_SEQ_BY_PROBE_SEARCH = "SELECT distinct a._Accession_key, "
				+ " ac.accID, a._Object_key, 'SEQUENCE' as _MGIType_key,"
				+ " a.preferred, ac._LogicalDB_key"
				+ " FROM ACC_Accession a, SEQ_Sequence s,"
				+ " SEQ_Probe_Cache spc, acc_accession ac"
				+ " where a.private != 1 and a._MGIType_key = 19 and"
				+ " a._Object_key = s._Sequence_key and s._Organism_key = 1"
				+ " and a._Object_key = spc._Sequence_key and"
				+ " spc._Probe_key = ac._Object_key and ac._MGIType_key = 3"
				+ " and AC._LogicalDB_key != 9 and ac.private != 1";

		// Gather the data

		ResultSet rs_seq_by_probe = executor.executeMGD(OTHER_SEQ_BY_PROBE_SEARCH);
		rs_seq_by_probe.next();

		log.info("Time taken gather sequence by probe id data set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_seq_by_probe.isAfterLast()) {

			builder.setType(rs_seq_by_probe.getString("_MGIType_key"));
			builder.setData(rs_seq_by_probe.getString("accID"));
			builder.setDb_key(rs_seq_by_probe.getString("_Object_key"));
			builder.setAccessionKey(rs_seq_by_probe.getString("_Accession_key"));
			builder.setPreferred(rs_seq_by_probe.getString("preferred"));
			builder.setProvider(phm.get(rs_seq_by_probe.getString("_LogicalDB_key")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();

			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}

			rs_seq_by_probe.next();
		}

		// Clean up

		log.info("Done creating documents for sequences by probe IDs!");

		rs_seq_by_probe.close();
	}

	/**
	 * Gather the orthologs data. This has a realized logical db display field.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doOrthologs() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Get accession IDs for non-mouse markers involved in HomoloGene
		// homology classes.

		String OTHER_ORTHOLOG_SEARCH = "select distinct aa._Accession_key, "
				+ " aa.accID, "
				+ " mm._Marker_key, "
				+ " 'ORTHOLOG' as _MGIType_key, "
				+ " aa.preferred, "
				+ " aa._LogicalDB_key, "
				+ " mo.commonName, "
				+ " hg.accID as HomoloGeneID "
				+ "from VOC_Term source, "
				+ " MRK_Cluster mc, "
				+ " MRK_ClusterMember mcm, "
				+ " MRK_Marker mm, "
				+ " MGI_Organism mo, "
				+ " ACC_Accession aa, "
				+ " ACC_Accession hg "
				+ "where source.term = 'HomoloGene' "
				+ " and source._Term_key = mc._ClusterSource_key "
				+ " and mc._Cluster_key = mcm._Cluster_key "
				+ " and mcm._Marker_key = mm._Marker_key "
				+ " and mm._Organism_key != 1 "
				+ " and mm._Organism_key = mo._Organism_key "
				+ " and mm._Marker_key = aa._Object_key "
				+ " and aa._MGIType_key = 2 "
				+ " and aa.private = 0"
				+ " and mc._Cluster_key = hg._Object_key "
				+ " and hg._MGIType_key = 39 "
				+ " and hg.private = 0";

		// gather the data

		ResultSet rs_orthologs = executor.executeMGD(OTHER_ORTHOLOG_SEARCH);
		rs_orthologs.next();

		log.info("Time taken to gather homologous marker id data set: "
				+ executor.getTiming());

		// Parse it

		int documentCount = 0;
		while (!rs_orthologs.isAfterLast()) {
			documentCount++;

			builder.setType(rs_orthologs.getString("_MGIType_key"));
			builder.setData(rs_orthologs.getString("accID"));

			builder.setDb_key(rs_orthologs.getString("_Marker_key"));

			builder.setAccessionKey(rs_orthologs.getString("_Accession_key"));
			builder.setPreferred(rs_orthologs.getString("preferred"));

			// This has a realized provider string, we add in the species.

			builder.setProvider(phm.get(rs_orthologs.getString("_LogicalDB_key")) + " - " + InitCap.initCap(rs_orthologs.getString("commonName")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();

			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}

			rs_orthologs.next();
		}

		// Clean up

		log.info("Done creating " + documentCount
				+ " documents for homologous marker IDs!");
		rs_orthologs.close();

		doHomoloGeneClasses();
	}

	/**
	 * Gather the HomoloGene class data. This has a realized logical db display
	 * field.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doHomoloGeneClasses() throws SQLException, InterruptedException {
		// Get accession IDs for the HomoloGene classes themselves.

		String HOMOLOGENE_CLUSTER_SEARCH =
				"select distinct aa._Accession_key, "
						+ " aa._Object_key, "
						+ " 'HOMOLOGY' as _MGIType_key, "
						+ " aa.preferred, "
						+ " aa._LogicalDB_key, "
						+ " aa.accID as HomoloGeneID "
						+ "from VOC_Term source, "
						+ " MRK_Cluster mc, "
						+ " ACC_Accession aa "
						+ "where source.term = 'HomoloGene' "
						+ " and source._Term_key = mc._ClusterSource_key "
						+ " and mc._Cluster_key = aa._Object_key "
						+ " and aa._MGIType_key = 39 "
						+ " and aa.private = 0";

		// gather the data

		ResultSet rs_homologene = executor.executeMGD(HOMOLOGENE_CLUSTER_SEARCH);
		rs_homologene.next();

		log.info("Time taken to gather HomoloGene class id data set: " + executor.getTiming());

		// Parse it

		int documentCount = 0;
		while (!rs_homologene.isAfterLast()) {
			documentCount++;

			builder.setType(IndexConstants.OTHER_HOMOLOGY);
			builder.setData(rs_homologene.getString("HomoloGeneID"));
			builder.setDb_key(rs_homologene.getString("HomoloGeneID"));
			builder.setAccessionKey(rs_homologene.getString("_Accession_key"));
			builder.setPreferred(rs_homologene.getString("preferred"));
			builder.setProvider(phm.get(rs_homologene.getString("_LogicalDB_key")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();

			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}

			rs_homologene.next();
		}

		// Clean up

		log.info("Done creating " + documentCount
				+ " documents for HomoloGene class IDs!");
		rs_homologene.close();
	}

	/**
	 * Gather Adult Mouse Anatomy data
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAMA() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up the adult mouse anatomy term accession id's, only for the
		// preferred accession id's

		String OTHER_AMA_SEARCH = "SELECT distinct a._Accession_key, a.accID, "
				+ "a._Object_key, 'AMA' as _MGIType_key, "
				+ "a.preferred, a._LogicalDB_key"
				+ " from acc_accession a, VOC_Term v"
				+ " where a.private !=1 and a._MGIType_key = 13 and "
				+ "a._Object_key = v._Term_key and v._Vocab_key = 6 "
				+ "and a.preferred = 1";

		// Gather the data

		ResultSet rs_ama = executor.executeMGD(OTHER_AMA_SEARCH);
		rs_ama.next();

		log.info("Time taken gather ama data set: " + executor.getTiming());

		// Parse it

		while (!rs_ama.isAfterLast()) {

			builder.setType(rs_ama.getString("_MGIType_key"));
			builder.setData(rs_ama.getString("accID"));
			builder.setDb_key(rs_ama.getString("_Object_key"));
			builder.setAccessionKey(rs_ama.getString("_Accession_key"));
			builder.setPreferred(rs_ama.getString("preferred"));
			builder.setProvider(phm.get(rs_ama.getString("_LogicalDB_key")));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
			rs_ama.next();
		}

		// Clean up

		log.info("Done creating documents for AMA!");
		rs_ama.close();

	}

}

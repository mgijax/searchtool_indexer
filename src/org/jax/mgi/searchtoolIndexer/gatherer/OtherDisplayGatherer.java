package org.jax.mgi.searchtoolIndexer.gatherer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Iterator;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.OtherDisplayLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * Gatherer responsible for understanding how to gather all needed information
 * that goes into the otherDisplay index.
 * 
 * Please note that all "Other" gatherers have a gathering item limit programmed
 * into each of their subsections. Based on this value, they are only able to
 * put a set maximum number of documents onto the stack before being forced to
 * wait.
 * 
 * This number is a configurable item via the Configuration file.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Responsible for running through the algorithm that gathers up all of
 *       the data needed to make the other display index. For each subsection,
 *       we parse through it placing Lucene documents on the stack for each row,
 *       and then perform cleanup.
 * 
 *       When all of the subsections are complete, we notify the stack that
 *       gathering is complete, perform some overall cleanup, and exit.
 */

public class OtherDisplayGatherer extends DatabaseGatherer {

	// Class Variables

	private double							total				= 0;

	/*
	 * Since other gathering is so long, it has extra log messages, these
	 * variable will control how often these log messages are displayed.
	 * 
	 * One the output_threshhold has been reached, we output one of these
	 * special log messages. We then increment the threshold by the output
	 * incrementer, and repeat the process.
	 */

	private double							output_incrementer	= 100000;
	private double							output_threshold	= 100000;

	// Create the one LuceneDocBuilder that this object will use.

	private OtherDisplayLuceneDocBuilder	builder				= new OtherDisplayLuceneDocBuilder();

	public OtherDisplayGatherer(IndexCfg config) {
		super(config);
	}

	/**
	 * This method encapsulates the list of tasks that need to be completed in
	 * order to gather the information for the OtherDisplay index. Once it is
	 * invoked, each object type will be collected in sequence, until all the
	 * indexes information is gathered, at which point it sets the gathering
	 * state to complete in the shared document stack, and exits.
	 */

	public void runLocal() throws Exception {
		doOrthologs();
		doProbes();
		doAssays();
		doReferences();
		doSequences();
		doGenotypes();
		doAntibodies();
		doExperiments();
		doImages();
		doAMA();
	}

	// get a mapping from each (String) genotype key to a String with the
	// comma-separate marker symbols for its allele pairs.  Only includes
	// genotypes with phenotype or disease annotations.
	private Map<String,String> getMarkers()
	    throws SQLException, InterruptedException {
	        String MARKERS = "select distinct g._Genotype_key, m.symbol "
		    + "from gxd_genotype g, gxd_allelegenotype gag, "
		    + "  mrk_marker m "
		    + "where g._Genotype_key = gag._Genotype_key "
		    + "  and gag._Marker_key = m._Marker_key "
		    + "  and exists (select 1 from voc_annot va "
		    + "    where va._AnnotType_key in (1002,1005) "
		    + "    and va._Object_key = g._Genotype_key) "
		    + "order by g._Genotype_key, m.symbol";

		ResultSet rs = executor.executeMGD(MARKERS);

		Map<String,String> m = new HashMap<String,String>();

		int mCt = 0;	// count of markers included in genotypes
		int gCt = 0;	// count of genotypes

		while (!rs.next()) {
		    String genotypeKey = rs.getString("_Genotype_key");
		    String symbol = rs.getString("symbol");

		    mCt++;

		    if (m.containsKey(genotypeKey)) {
			m.put(genotypeKey, m.get(genotypeKey) + ", " + symbol);
		    } else {
			m.put(genotypeKey, symbol);
			gCt++;
		    }
		}
		log.info("Got " + mCt + " markers in " + gCt + " genotypes");
		return m;
	}

	private void doGenotypes() throws SQLException, InterruptedException {
		Map<String,String> markers = getMarkers();

		String OTHER_GENOTYPE_DISPLAY = "select gg._genotype_key, '"
		    + IndexConstants.OTHER_GENOTYPE
		    + "' as type, a.accid "
		    + "from gxd_genotype gg, acc_accession a "
		    + "where gg._genotype_key = a._object_key "
		    + "  and a._mgitype_key = 12"
		    + "  and exists (select 1 from voc_annot va "
		    + "    where va._AnnotType_key in (1002,1005) "
		    + "    and va._Object_key = gg._Genotype_key)";

		ResultSet rs = executor.executeMGD(OTHER_GENOTYPE_DISPLAY);

		log.info("Time taken gather genotype result set: " + executor.getTiming());

		double startCount = total;

		// Parse it

		while (rs.next()) {
			String genotypeKey = rs.getString("_Genotype_key");

			builder.setDb_key(genotypeKey);
			builder.setData(rs.getString("accid"));
			builder.setDataType(rs.getString("type"));

			if (markers.containsKey(genotypeKey)) {
			    builder.setName("Involving: " + markers.get(genotypeKey));
			} else {
			    builder.setName("Genotype " + rs.getString("accid"));
			}
			
			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Genotypes (" + (total - startCount) + ")!");
		rs.close();
	}

	/**
	 * Gather the probe data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doProbes() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up probe key, name and probe type.

		String OTHER_PROBE_DISPLAY_KEY = "select distinct _Probe_key, '"
				+ IndexConstants.OTHER_PROBE + "' as type, name, vt.Term"
				+ " from PRB_Probe_view, voc_term vt"
				+ " where _SegmentType_key = vt._Term_key";

		// Gather the data

		ResultSet rs = executor.executeMGD(OTHER_PROBE_DISPLAY_KEY);

		log.info("Time taken gather probe result set: " + executor.getTiming());

		// Parse it

		while (rs.next()) {

			builder.setDb_key(rs.getString("_Probe_key"));
			builder.setDataType(rs.getString("type"));
			builder.setQualifier(rs.getString("Term"));
			builder.setName(rs.getString("name"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Probes!");
		rs.close();

	}

	/**
	 * Gather the assay data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAssays() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up assay name, symbol and type

		String OTHER_ASSAY_DISPLAY_KEY = "select _Assay_key, '"
				+ IndexConstants.OTHER_ASSAY
				+ "' as type, name, symbol from GXD_Assay_View";

		// Gather the data

		ResultSet rs_assay = executor.executeMGD(OTHER_ASSAY_DISPLAY_KEY);

		log.info("Time taken gather ASSAY result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_assay.next()) {

			builder.setDb_key(rs_assay.getString("_Assay_key"));
			builder.setDataType(rs_assay.getString("type"));
			builder.setName(rs_assay.getString("symbol") + ", "
					+ rs_assay.getString("name"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Assay!");
		rs_assay.close();
	}

	/**
	 * Gather the reference data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doReferences() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up reference key, type, and short citation

		String OTHER_REF_DISPLAY_KEY = "select distinct _Refs_key, '"
				+ IndexConstants.OTHER_REFERENCE
				+ "' as type, short_citation from BIB_View";

		// Gather the data

		ResultSet rs_ref = executor.executeMGD(OTHER_REF_DISPLAY_KEY);

		log.info("Time taken gather reference result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_ref.next()) {

			builder.setDb_key(rs_ref.getString("_Refs_key"));
			builder.setDataType(rs_ref.getString("type"));
			builder.setName(rs_ref.getString("short_citation"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done References!");
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

		// gather up sequence key, type and description

		String OTHER_SEQ_DISPLAY_KEY = "select _Sequence_key, '"
				+ IndexConstants.OTHER_SEQUENCE
				+ "' as type, s.description, v1.term AS sequenceType"
				+ " FROM SEQ_Sequence s, VOC_Term v1"
				+ " WHERE s._sequencetype_key = v1._term_key";

		// Gather the data

		ResultSet rs_seq = executor.executeMGD(OTHER_SEQ_DISPLAY_KEY);

		log.info("Time taken gather sequence result set: " + executor.getTiming());

		// Parse it

		while (rs_seq.next()) {

			builder.setDb_key(rs_seq.getString("_Sequence_key"));
			builder.setDataType(rs_seq.getString("type"));
			builder.setQualifier(rs_seq.getString("sequenceType"));
			if (rs_seq.getString("description") != null) {
				builder.setName(rs_seq.getString("description"));
			}

			// Place a document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Sequences!");
		rs_seq.close();
	}

	/**
	 * Gather Ortholog data. Please note that this has a realized display field.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doOrthologs() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up marker key, ortholog symbol and name.  Note that Alliance clusters
		// do not have IDs, so we'll substitute a cluster key in this case.

		String OTHER_ORTHOLOG_DISPLAY = "select distinct mc._Cluster_key as accID, "
				+ " mv._Marker_key, "
				+ " mv.symbol, "
				+ " mv.name "
				+ "from MRK_Marker_View mv, "
				+ " MRK_ClusterMember mcm, "
				+ " MRK_Cluster mc, "
				+ " VOC_Term vt "
				+ "where mv._Organism_key != 1 "
				+ " and mv._Marker_key = mcm._Marker_key "
				+ " and mcm._Cluster_key = mc._Cluster_key "
				+ " and mc._ClusterSource_key = vt._Term_key "
				+ " and vt.abbreviation = 'Alliance Direct' ";

		// Gather the data

		ResultSet rs_ortho = executor.executeMGD(OTHER_ORTHOLOG_DISPLAY);
		
		log.info("Time taken gather homologous marker result set: "
				+ executor.getTiming());

		// Parse it

		int documentCount = 0;
		Set<String> clusters = new HashSet<String>();
		
		while (rs_ortho.next()) {
			documentCount++;
			clusters.add(rs_ortho.getString("accID"));

			builder.setDb_key(rs_ortho.getString("_Marker_key"));
			builder.setDataType(IndexConstants.OTHER_ORTHOLOG);

			// The name for Orthologs is a realized field.

			String symbol = new String(rs_ortho.getString("symbol"));
			String name = new String(rs_ortho.getString("name"));

			builder.setName(symbol + ", " + name);

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done homologous markers! (" + documentCount + " documents for " + clusters.size() + "clusters)");
		rs_ortho.close();

		doHomologyClasses();
	}

	/**
	 * Gather display data for homology classes. Please note that this has a
	 * realized display field.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doHomologyClasses() throws SQLException, InterruptedException {
		HashMap descriptions = getHomologyClassDescriptions();

		// SQL for this Subsection

		// gather up marker key, type, ortholog symbol and name

		String HOMOLOGY_CLASSES_DISPLAY =
				"select distinct mc._Cluster_key as accID, "
						+ " '" + IndexConstants.OTHER_HOMOLOGY + "' as type "
						+ "from VOC_Term source, "
						+ " MRK_Cluster mc "
						+ "where source.abbreviation = 'Alliance Direct' "
						+ " and source._Term_key = mc._ClusterSource_key ";

		// Gather the data

		ResultSet rs_ortho = executor.executeMGD(HOMOLOGY_CLASSES_DISPLAY);
		
		log.info("Time taken gather homology class result set: "
				+ executor.getTiming());

		String hgID = null;
		Set<String> clusters = new HashSet<String>();

		int documentCount = 0;
		while (rs_ortho.next()) {
			documentCount++;

			hgID = rs_ortho.getString("accID");
			builder.setDb_key(hgID);
			builder.setDataType(rs_ortho.getString("type"));
			clusters.add(hgID);

			if (descriptions.containsKey(hgID)) {
				builder.setName((String) descriptions.get(hgID));
			} else {
				builder.setName("homology class: " + hgID);
			}

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done homology classes! (" + documentCount + " documents for " + clusters.size() + " clusters)");
		rs_ortho.close();
	}

	/**
	 * compose and return the homology class descriptions
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private HashMap getHomologyClassDescriptions() throws SQLException, InterruptedException {

		log.info("Building homology class descriptions");

		// key is homology ID, contents is a HashMap that maps from organism
		// to a count of markers for that organism in the homology class
		HashMap organismsById = new HashMap();

		String HOMOLOGY_CLASS_SEARCH = "select mc._Cluster_key as accID, "
				+ " mo.commonName, "
				+ " count(1) as idCount "
				+ "from MRK_Cluster mc, "
				+ " VOC_Term vt, "
				+ " MRK_ClusterMember mcm, "
				+ " MRK_Marker mm, "
				+ " MGI_Organism mo "
				+ "where vt.abbreviation = 'Alliance Direct' "
				+ " and vt._Term_key = mc._ClusterSource_key "
				+ " and mc._Cluster_key = mcm._Cluster_key "
				+ " and mcm._Marker_key = mm._Marker_key "
				+ " and mm._Organism_key = mo._Organism_key "
				+ "group by mc._Cluster_key, mo.commonName";

		ResultSet rs_counts = executor.executeMGD(HOMOLOGY_CLASS_SEARCH);

		log.info("Time taken to gather organism counts for homology classes: " + executor.getTiming());

		// keyed by organism name, values are counts of markers for that
		// organism in the homology class we are considering
		HashMap inner = null;

		String hgID = null; // homology ID
		String organism = null; // common name for organism
		String count = null; // integer count of markers
		Set<String> clusters = new HashSet<String>();

		int rowCount = 0;

		while (rs_counts.next()) {
			rowCount++;

			hgID = rs_counts.getString("accID");
			organism = rs_counts.getString("commonName");
			count = rs_counts.getString("idCount");
			clusters.add(hgID);

			if (organismsById.containsKey(hgID)) {
				inner = (HashMap) organismsById.get(hgID);
			} else {
				inner = new HashMap();
				organismsById.put(hgID, inner);
			}
			inner.put(organism, count);

		}
		rs_counts.close();

		log.info("Processed " + rowCount + " rows for " +
				organismsById.size() + " homology classes (" + clusters.size() + " clusters)");

		// So we now have essentially:
		// { homology ID : { organism : count of markers } }
		// and we need to transform that into a 1-line description of each
		// homology class.

		// keys are homology IDs, values are description strings
		HashMap descriptions = new HashMap();

		// organisms which need to be renamed
		HashMap renamed = new HashMap();
		renamed.put("mouse, laboratory", "mouse");
		renamed.put("dog, domestic", "dog");

		// organisms given priority in ordering
		ArrayList priority = new ArrayList();

		// the big three
		priority.add("human");
		priority.add("mouse, laboratory");
		priority.add("rat");

		// primates - alphabetical
		priority.add("chimpanzee");
		priority.add("rhesus macaque");

		// mammals - alphabetical
		priority.add("cattle");
		priority.add("dog, domestic");

		// others - alphabetical
		priority.add("chicken");
		priority.add("western clawed frog");
		priority.add("zebrafish");

		String displayOrg = null;
		StringBuffer sb = null;
		Set orgSet = null;
		Iterator orgIt = null;
		String description = null;

		Iterator it = organismsById.keySet().iterator();
		while (it.hasNext()) {

			hgID = (String) it.next();
			inner = (HashMap) organismsById.get(hgID);
			sb = new StringBuffer();
			sb.append("Class with ");

			// do the organisms named in priority order first

			orgIt = priority.iterator();
			while (orgIt.hasNext()) {
				organism = (String) orgIt.next();

				if (inner.containsKey(organism)) {
					if (renamed.containsKey(organism)) {
						displayOrg = (String) renamed.get(organism);
					} else {
						displayOrg = organism;
					}
					sb.append((String) inner.get(organism)); // count
					sb.append(" ");
					sb.append(displayOrg); // organism
					sb.append(", ");
				}
			}

			// fill in any extra organisms that were not prioritized in an
			// ad-hoc manner

			orgIt = inner.keySet().iterator();
			while (orgIt.hasNext()) {
				organism = (String) orgIt.next();

				if (!priority.contains(organism)) {
					if (renamed.containsKey(organism)) {
						displayOrg = (String) renamed.get(organism);
					} else {
						displayOrg = organism;
					}
					sb.append((String) inner.get(organism));
					sb.append(" ");
					sb.append(displayOrg);
					sb.append(", ");
				}

			}

			sb.append("genes");
			description = sb.toString();

			// We now have the basic description string compiled, so do last
			// minute tweaking, including:
			// 1. removing a trailing comma from the list of organisms
			// 2. adding an 'and' between the last two organisms
			// 3. converting 'genes' to 'gene' for classes with only 1 marker

			description = description.replace(", genes", "");

			int organismCount = ((HashMap) organismsById.get(hgID)).size();

			if (organismCount == 2) {
				// if only two organisms, strip the comma
				description = description.replace(", ", " and ");

			} else if (organismCount >= 3) {
				// if three or more organisms, leave the comma & insert 'and'
				int lastComma = description.lastIndexOf(", ");
				description = description.substring(0, lastComma)
						+ ", and "
						+ description.substring(lastComma + 2);
			}

			descriptions.put(hgID, description);

		} // while (it.hasNext())

		log.info("Finished building " + descriptions.size() + " homology class descriptions");
		return descriptions;
	}

	/**
	 * Gather the antibody data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAntibodies() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up antibody key, name and type

		String OTHER_ANTIBODY_DISPLAY_KEY = "select distinct _Antibody_key, '"
				+ IndexConstants.OTHER_ANTIBODY
				+ "' as type, antibodyName from GXD_Antibody";

		// Gather the data

		ResultSet rs_antibody = executor.executeMGD(OTHER_ANTIBODY_DISPLAY_KEY);
		
		log.info("Time taken gather antibody result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_antibody.next()) {

			builder.setDb_key(rs_antibody.getString("_Antibody_key"));
			builder.setDataType(rs_antibody.getString("type"));
			builder.setName(rs_antibody.getString("antibodyName"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Antibodies!");
		rs_antibody.close();
	}


	/**
	 * Gather experiment data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doExperiments() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up experiment key, citation and type

		String OTHER_EXPERIMENT_DISPLAY_KEY = "select _Expt_key, '"
				+ IndexConstants.OTHER_EXPERIMENT
				+ "' as type, short_citation, exptType from MLD_Expt_View";

		// Gather the data

		ResultSet rs_experiment = executor.executeMGD(OTHER_EXPERIMENT_DISPLAY_KEY);
		
		log.info("Time taken gather experiment result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_experiment.next()) {

			builder.setDb_key(rs_experiment.getString("_Expt_key"));
			builder.setDataType(rs_experiment.getString("type"));
			builder.setQualifier(rs_experiment.getString("exptType"));
			builder.setName(rs_experiment.getString("short_citation"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();

		}

		// Clean up

		log.info("Done Experiments!");
		rs_experiment.close();
	}

	/**
	 * Gather the image data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doImages() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// gather up image key, citation and type

		String OTHER_IMAGE_DISPLAY_KEY = "select _Image_key, '"
				+ IndexConstants.OTHER_IMAGE
				+ "' as type, short_citation  from IMG_Image_View";

		// Gather the data

		ResultSet rs_image = executor.executeMGD(OTHER_IMAGE_DISPLAY_KEY);
		
		log.info("Time taken gather image result set: "
				+ executor.getTiming());

		// parse it

		while (rs_image.next()) {

			builder.setDb_key(rs_image.getString("_Image_key"));
			builder.setDataType(rs_image.getString("type"));
			builder.setName(rs_image.getString("short_citation"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done Images!");
		rs_image.close();
	}

	/**
	 * Collect the Adult Mouse Anatomy data.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAMA() throws SQLException, InterruptedException {

		// SQL for this Subsection

		// Gather up adult mouse anatomy term keys, type and term.

		String OTHER_AMA_DISPLAY = "select _Term_key, '"
				+ IndexConstants.OTHER_AMA + "' as type, term"
				+ " from VOC_Term" + " where _Vocab_key = 6 ";

		// Gather the data

		ResultSet rs_ama = executor.executeMGD(OTHER_AMA_DISPLAY);
		
		log.info("Time taken gather AMA result set: "
				+ executor.getTiming());

		// Parse it

		while (rs_ama.next()) {

			builder.setDb_key(rs_ama.getString("_Term_key"));
			builder.setDataType(rs_ama.getString("type"));
			builder.setName(rs_ama.getString("term"));

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total
						+ " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
		}

		// Clean up

		log.info("Done AMA!");
		rs_ama.close();
	}

}

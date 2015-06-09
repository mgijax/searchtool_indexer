package org.jax.mgi.searchtoolIndexer.gatherer;

import java.util.HashMap;
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
		rs.next();

		Map<String,String> m = new HashMap<String,String>();

		int mCt = 0;	// count of markers included in genotypes
		int gCt = 0;	// count of genotypes

		while (!rs.isAfterLast()) {
		    String genotypeKey = rs.getString("_Genotype_key");
		    String symbol = rs.getString("symbol");

		    mCt++;

		    if (m.containsKey(genotypeKey)) {
			m.put(genotypeKey, m.get(genotypeKey) + ", " + symbol);
		    } else {
			m.put(genotypeKey, symbol);
			gCt++;
		    }
		    rs.next();
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
		rs.next();

		log.info("Time taken gather genotype result set: " + executor.getTiming());

		double startCount = total;

		// Parse it

		while (!rs.isAfterLast()) {
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
			rs.next();
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
		rs.next();

		log.info("Time taken gather probe result set: " + executor.getTiming());

		// Parse it

		while (!rs.isAfterLast()) {

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
			rs.next();
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
		rs_assay.next();
		log.info("Time taken gather ASSAY result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_assay.isAfterLast()) {

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
			rs_assay.next();
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

		rs_ref.next();

		log.info("Time taken gather reference result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_ref.isAfterLast()) {

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
			rs_ref.next();
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

		rs_seq.next();
		log.info("Time taken gather sequence result set: " + executor.getTiming());

		// Parse it

		while (!rs_seq.isAfterLast()) {

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
			rs_seq.next();
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

		// gather up marker key, HomoloGene ID, ortholog symbol and name.

		String OTHER_ORTHOLOG_DISPLAY = "select distinct a.accID, "
				+ " mv._Marker_key, "
				+ " mv.symbol, "
				+ " mv.name "
				+ "from MRK_Marker_View mv, "
				+ " MRK_ClusterMember mcm, "
				+ " ACC_Accession a, "
				+ " MRK_Cluster mc, "
				+ " VOC_Term vt "
				+ "where mv._Organism_key != 1 "
				+ " and mv._Marker_key = mcm._Marker_key "
				+ " and mcm._Cluster_key = a._Object_key "
				+ " and a._MGIType_key = 39 "
				+ " and mcm._Cluster_key = mc._Cluster_key "
				+ " and mc._ClusterSource_key = vt._Term_key "
				+ " and vt.term = 'HomoloGene' "
				+ " and a.private = 0 "
				+ " and a.preferred = 1";

		// Gather the data

		ResultSet rs_ortho = executor.executeMGD(OTHER_ORTHOLOG_DISPLAY);
		rs_ortho.next();
		log.info("Time taken gather homologous marker result set: "
				+ executor.getTiming());

		// Parse it

		int documentCount = 0;
		while (!rs_ortho.isAfterLast()) {
			documentCount++;

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
			rs_ortho.next();
		}

		// Clean up

		log.info("Done homologous markers! (" + documentCount + " documents)");
		rs_ortho.close();

		doHomoloGeneClasses();
	}

	/**
	 * Gather display data for HomoloGene classes. Please note that this has a
	 * realized display field.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doHomoloGeneClasses() throws SQLException, InterruptedException {
		HashMap descriptions = getHomoloGeneClassDescriptions();

		// SQL for this Subsection

		// gather up marker key, type, ortholog symbol and name

		String HOMOLOGENE_CLASSES_DISPLAY =
				"select distinct aa.accID as HomoloGeneID, "
						+ " '" + IndexConstants.OTHER_HOMOLOGY + "' as type "
						+ "from VOC_Term source, "
						+ " MRK_Cluster mc, "
						+ " ACC_Accession aa "
						+ "where source.term = 'HomoloGene' "
						+ " and source._Term_key = mc._ClusterSource_key "
						+ " and mc._Cluster_key = aa._Object_key "
						+ " and aa._MGIType_key = 39 "
						+ " and aa.private = 0";

		// Gather the data

		ResultSet rs_ortho = executor.executeMGD(HOMOLOGENE_CLASSES_DISPLAY);
		rs_ortho.next();
		log.info("Time taken gather HomoloGene class result set: "
				+ executor.getTiming());

		String hgID = null;

		int documentCount = 0;
		while (!rs_ortho.isAfterLast()) {
			documentCount++;

			hgID = rs_ortho.getString("HomoloGeneID");
			builder.setDb_key(hgID);
			builder.setDataType(rs_ortho.getString("type"));

			if (descriptions.containsKey(hgID)) {
				builder.setName((String) descriptions.get(hgID));
			} else {
				builder.setName("HomoloGene class: " + hgID);
			}

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			total++;
			if (total >= output_threshold) {
				log.debug("We have now gathered " + total + " documents!");
				output_threshold += output_incrementer;
			}
			builder.clear();
			rs_ortho.next();
		}

		// Clean up

		log.info("Done HomoloGene classes! (" + documentCount + " documents)");
		rs_ortho.close();
	}

	/**
	 * compose and return the HomoloGene class descriptions
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private HashMap getHomoloGeneClassDescriptions() throws SQLException, InterruptedException {

		log.info("Building HomoloGene class descriptions");

		// key is HomoloGene ID, contents is a HashMap that maps from organism
		// to a count of markers for that organism in the HomoloGene class
		HashMap organismsById = new HashMap();

		String HOMOLOGENE_CLASS_SEARCH = "select aa.accID, "
				+ " mo.commonName, "
				+ " count(1) as idCount "
				+ "from MRK_Cluster mc, "
				+ " VOC_Term vt, "
				+ " MRK_ClusterMember mcm, "
				+ " ACC_Accession aa, "
				+ " MRK_Marker mm, "
				+ " MGI_Organism mo "
				+ "where vt.term = 'HomoloGene' "
				+ " and vt._Term_key = mc._ClusterSource_key "
				+ " and mc._Cluster_key = mcm._Cluster_key "
				+ " and mcm._Marker_key = mm._Marker_key "
				+ " and mm._Organism_key = mo._Organism_key "
				+ " and mc._Cluster_key = aa._Object_key "
				+ " and aa._MGIType_key = 39 "
				+ " and aa.private = 0 "
				+ "group by aa.accID, mo.commonName";

		ResultSet rs_counts = executor.executeMGD(HOMOLOGENE_CLASS_SEARCH);
		rs_counts.next();

		log.info("Time taken to gather organism counts for HomoloGene classes: "
				+ executor.getTiming());

		// keyed by organism name, values are counts of markers for that
		// organism in the HomoloGene class we are considering
		HashMap inner = null;

		String hgID = null; // HomoloGene ID
		String organism = null; // common name for organism
		String count = null; // integer count of markers

		int rowCount = 0;

		while (!rs_counts.isAfterLast()) {
			rowCount++;

			hgID = rs_counts.getString("accID");
			organism = rs_counts.getString("commonName");
			count = rs_counts.getString("idCount");

			if (organismsById.containsKey(hgID)) {
				inner = (HashMap) organismsById.get(hgID);
			} else {
				inner = new HashMap();
				organismsById.put(hgID, inner);
			}
			inner.put(organism, count);

			rs_counts.next();
		}
		rs_counts.close();

		log.info("Processed " + rowCount + " rows for " +
				organismsById.size() + " HomoloGene classes");

		// So we now have essentially:
		// { HomoloGene ID : { organism : count of markers } }
		// and we need to transform that into a 1-line description of each
		// HomoloGene class.

		// keys are HomoloGene IDs, values are description strings
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

		log.info("Finished building " + descriptions.size() + " HomoloGene class descriptions");
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
		rs_antibody.next();
		log.info("Time taken gather antibody result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_antibody.isAfterLast()) {

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
			rs_antibody.next();
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
		rs_experiment.next();
		log.info("Time taken gather experiment result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_experiment.isAfterLast()) {

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
			rs_experiment.next();
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
		rs_image.next();
		log.info("Time taken gather image result set: "
				+ executor.getTiming());

		// parse it

		while (!rs_image.isAfterLast()) {

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
			rs_image.next();
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
		rs_ama.next();
		log.info("Time taken gather AMA result set: "
				+ executor.getTiming());

		// Parse it

		while (!rs_ama.isAfterLast()) {

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
			rs_ama.next();
		}

		// Clean up

		log.info("Done AMA!");
		rs_ama.close();
	}

}

package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureExactLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we might need
 * for the markerExact index. This currently consists of non symbol
 * nomenclature.
 * 
 * This information is then used to populate the markerExact index.
 * 
 * @author mhall
 * 
 * @has An instance of the IndexCfg object, which is used to setup this object.
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 *       Each component basically makes a call to the database and then starts
 *       parsing through its result set. For each record, we generate a Lucene
 *       document, and place it on the shared stack.
 * 
 *       After all of the components are finished, we notify the stack that
 *       gathering is complete, clean up our jdbc connections and exit.
 */

public class GenomeFeatureExactGatherer extends DatabaseGatherer {

	// Class Variables

	private GenomeFeatureExactLuceneDocBuilder builder = new GenomeFeatureExactLuceneDocBuilder();

	/**
	 * Create a new instance of the MarkerExactGatherer.
	 * 
	 * @param config
	 */

	public GenomeFeatureExactGatherer(IndexCfg config) {

		super(config);
	}

	/**
	 * This method encapsulates the algorithm used for gathering the data needed
	 * to create the MarkerExact documents.
	 */

	public void runLocal() throws Exception {
		doMarkerLabels();
		doAlleleLabels();
	}

	/**
	 * Grab all non symbol labels associated with markers (Alleles and orthologs
	 * included)
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doMarkerLabels() throws SQLException, InterruptedException {

		// Grab marker key, label, label type and the type name for all
		// marker, alleles and orthologs, but not the symbols.
		// Also only do this for the mouse related items, where the marker
		// has not been withdrawn.
		//
		// Includes: only data associated in MRK_Label with mouse markers
		// Excludes:
		// marker symbol
		// allele symbol
		// ortholog symbol
		// allele name
		// all data for withdrawn markers
		// all data for transgene markers

		String GENE_LABEL_EXACT = "select ml._Marker_key, "
				+ "ml.label, ml.labelType,  ml.labelTypeName, "
				+ "ml._OrthologOrganism_key, "
				+ "ml._Label_Status_key, ml._Label_key"
				+ " from MRK_Label ml, MRK_Marker m"
				+ " where  ml._Organism_key = 1 and ml._Marker_key = "
				+ "m._Marker_key and m._Marker_Status_key !=2"
				+ "and ml.labelType not in ('MS', 'AS', 'OS', 'AN') "
				+ "and m._Marker_Type_key != 12";

		// Gather the data

		ResultSet rs_label = executor.executeMGD(GENE_LABEL_EXACT);
		rs_label.next();

		log.info("Time taken to gather label's result set: "
				+ executor.getTiming());

		// Parse it

		String displayType = "";
		String dataType = "";
		while (!rs_label.isAfterLast()) {

			dataType = rs_label.getString("labelType");

			if (dataType.equals(IndexConstants.MARKER_SYNOYNM)
					&& rs_label.getString("_OrthologOrganism_key") != null) {
				builder.setDataType(IndexConstants.ORTHOLOG_SYNONYM);
			} else {
				builder.setDataType(dataType);
			}

			if (!rs_label.getString("_Label_Status_key").equals("1")) {

				// If we have an old bit of nomen, we need to create a
				// custom type.

				builder.setDataType(builder.getDataType() + "O");
			}

			builder.setData(rs_label.getString("label"));
			builder.setDb_key(rs_label.getString("_Marker_key"));
			builder.setObject_type("MARKER");
			builder.setUnique_key(rs_label.getString("_Label_key") + IndexConstants.MARKER_TYPE_NAME);
			displayType = InitCap.initCap(rs_label.getString("labelTypeName"));

			// A manual adjustment of the display type for a special case.

			if (displayType.equals("Current Name")) {
				displayType = "Name";
			}

			builder.setDisplay_type(displayType);

			// Add the document to the stack

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_label.next();
		}

		// Clean up

		rs_label.close();

		log.info("Done Labels!");

	}

	/**
	 * Grab all non symbol labels associated with markers (Alleles and orthologs
	 * included)
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAlleleLabels() throws SQLException, InterruptedException {

		// Grab marker key, label, label type and the type name for all
		// marker, alleles and orthologs, but not the symbols.
		// Also only do this for the mouse related items, where the marker
		// has not been withdrawn.
		//
		// Includes: current allele names and synonyms from ALL_Label,
		// Excludes:
		// all data for wild-type alleles
		// allele symbols

		String ALLELE_LABEL_EXACT = "select distinct aa._Allele_key, "
			+ "  m.name, "
			+ "  al.label, "
			+ "  al.labelType, "
			+ "  al.labelTypeName, "
			+ "  al._Label_Status_key "
			+ "from ALL_Allele aa "
			+ "inner join ALL_Label al on (al._Allele_key = aa._Allele_key "
			+ "  and al._Label_Status_key != 0 "
			+ "  and al.labelType in ('AN', 'AY') ) "
			+ "left outer join MRK_Marker m on ("
			+ "  aa._Marker_key = m._Marker_key)"
			+ "where aa.isWildType != 1 ";

		// Gather the data

		ResultSet rs_label = executor.executeMGD(ALLELE_LABEL_EXACT);
		rs_label.next();

		log.info("Time taken to gather label's result set: "
				+ executor.getTiming());

		// Parse it

		String displayType = "";
		String dataType = "";
		while (!rs_label.isAfterLast()) {

			dataType = rs_label.getString("labelType");

			builder.setDataType(dataType);

			if (!rs_label.getString("_Label_Status_key").equals("1")) {

				// If we have an old bit of nomen, we need to create a
				// custom type.

				builder.setDataType(builder.getDataType() + "O");
			}

			builder.setData(rs_label.getString("label"));
			builder.setDb_key(rs_label.getString("_Allele_key"));
			builder.setObject_type("ALLELE");
			builder.setUnique_key(rs_label.getString("_allele_key") + rs_label.getString("label") + rs_label.getString("labelType") + IndexConstants.ALLELE_TYPE_NAME);
			displayType = InitCap.initCap(rs_label.getString("labelTypeName"));

			// A manual adjustment of the display type for a special case.

			if (displayType.equals("Current Name")) {
				displayType = "Name";
			}

			builder.setDisplay_type(displayType);

			// Add the document to the stack

			documentStore.push(builder.getDocument());

			if (rs_label.getString("labelType").equals("AN")) {

				if (rs_label.getString("name") != null && (!rs_label.getString("name").equals(rs_label.getString("label")))) {
					builder.setData(rs_label.getString("name") + "; " + rs_label.getString("label"));
					documentStore.push(builder.getDocument());
				}
			}

			builder.clear();
			rs_label.next();
		}

		// Clean up

		rs_label.close();

		log.info("Done Labels!");

	}
}

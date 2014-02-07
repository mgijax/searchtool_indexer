package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureSymbolLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up any information that we might need
 * for the marker symbol index. This is therefore restricted to just symbols for
 * markers/alleles and orthologs.
 * 
 * This information is then used to populate the markerSymbol index.
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

public class GenomeFeatureSymbolGatherer extends DatabaseGatherer {

	// Class Variables

	private GenomeFeatureSymbolLuceneDocBuilder builder = new GenomeFeatureSymbolLuceneDocBuilder();

	/**
	 * Create a new instance of the MarkerExactGatherer, and populate its
	 * translation hashmaps.
	 * 
	 * @param config
	 */

	public GenomeFeatureSymbolGatherer(IndexCfg config) {
		super(config);
	}

	/**
	 * This method encapsulates the algorithm used for gathering the data needed
	 * to create a MarkerExact document.
	 */

	public void runLocal() throws Exception {
		doMarkerSymbols();
		doAlleleSymbols();
	}

	/**
	 * Grab all labels associated with markers (Alleles and orthologs included)
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doMarkerSymbols() throws SQLException, InterruptedException {

		// Gather up all the marker, allele and ortholog symbols, where the
		// symbol is for mouse, and the marker has not been withdrawn.
		//
		// Includes: (for current mouse markers)
		// marker names and synonyms
		// ortholog names
		// allele symbols and names
		// Excludes:
		// all data for transgene markers
		// marker symbols
		// ortholog symbols

		String GENE_LABEL_EXACT = "select ml._Marker_key, "
				+ "ml.label, ml._OrthologOrganism_key, ml.labelType,"
				+ " ml.labelTypeName, ml._Label_Status_key," + " ml._Label_key"
				+ " from MRK_Label ml, MRK_Marker m"
				+ " where  ml._Organism_key = 1 and ml._Marker_key = "
				+ "m._Marker_key and m._Marker_Status_key !=2"
				+ "and ml.labelType in ('MS', 'OS') "
				+ " and m._Marker_Type_key != 12";

		// Gather the data

		ResultSet rs_label = executor.executeMGD(GENE_LABEL_EXACT);
		rs_label.next();

		log.info("Time taken to gather label's result set: "
				+ executor.getTiming());

		// Parse it

		String displayType = "";

		while (!rs_label.isAfterLast()) {

			if (rs_label.getString("labelType").equals(
					IndexConstants.ORTHOLOG_SYMBOL)) {
				String organism = rs_label.getString("_OrthologOrganism_key");

				// There is a special case where we want to define a new type
				// for human and rat symbols.

				if (organism != null && organism.equals("2")) {
					builder.setDataType(IndexConstants.ORTHOLOG_SYMBOL_HUMAN);
				} else if (organism != null && organism.equals("40")) {
					builder.setDataType(IndexConstants.ORTHOLOG_SYMBOL_RAT);
				} else {
					builder.setDataType(rs_label.getString("labelType"));
				}
			} else {
				builder.setDataType(rs_label.getString("labelType"));
			}

			// If we have an old symbol, we need to create a custom type.

			if (!rs_label.getString("_Label_Status_key").equals("1")) {

				builder.setDataType(builder.getDataType() + "O");
			}

			builder.setData(rs_label.getString("label"));
			builder.setRaw_data(rs_label.getString("label"));
			builder.setDb_key(rs_label.getString("_Marker_key"));
			builder.setUnique_key(rs_label.getString("_Label_key")
					+ IndexConstants.MARKER_TYPE_NAME);
			builder.setObject_type("MARKER");
			displayType = InitCap.initCap(rs_label.getString("labelTypeName"));
			if (displayType.equals("Current Symbol")) {
				displayType = "Symbol";
			}
			builder.setDisplay_type(displayType);

			// Place the document on the stack.

			documentStore.push(builder.getDocument());
			builder.clear();
			rs_label.next();
		}

		// Clean up

		rs_label.close();

		log.info("Done Labels!");
	}

	/**
	 * Grab all labels associated with markers (Alleles and orthologs included)
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */

	private void doAlleleSymbols() throws SQLException, InterruptedException {

		// Gather up all the marker, allele and ortholog symbols, where the
		// symbol is for mouse, and the marker has not been withdrawn.
		//
		// Includes: current allele symbols
		// Excludes: all data for wild-type alleles

		String GENE_LABEL_EXACT = "select distinct aa._Allele_key, "
				+ "al.label, al.labelType, al.labelTypeName, al._Label_Status_key "
				+ " from all_label al, ALL_Allele aa"
				+ " where al._Allele_key ="
				+ " aa._Allele_key and al._Label_Status_key != 0 "
				+ " and al.labelType = 'AS' and aa.isWildType != 1";

		// Gather the data

		ResultSet rs_label = executor.executeMGD(GENE_LABEL_EXACT);
		rs_label.next();

		log.info("Time taken to gather allele label's result set: "
				+ executor.getTiming());

		// Parse it

		String displayType = "";

		while (!rs_label.isAfterLast()) {

			builder.setDataType(rs_label.getString("labelType"));

			// If we have an old symbol, we need to create a custom type.

			if (!rs_label.getString("_Label_Status_key").equals("1")) {

				builder.setDataType(builder.getDataType() + "O");
			}

			String label = rs_label.getString("label");

			builder.setData(label);
			builder.setRaw_data(label);
			builder.setDb_key(rs_label.getString("_Allele_key"));
			builder.setUnique_key(rs_label.getString("_Allele_key") + rs_label.getString("label") + rs_label.getString("labelType") + IndexConstants.ALLELE_TYPE_NAME);
			builder.setObject_type("ALLELE");
			displayType = InitCap.initCap(rs_label.getString("labelTypeName"));
			if (displayType.equals("Current Symbol")) {
				displayType = "Symbol";
			}
			builder.setDisplay_type(displayType);

			// Place the document on the stack.

			documentStore.push(builder.getDocument());

			String fixedLabel = rs_label.getString("label").replace("<", "")
					.replace(">", "");

			if (!fixedLabel.equals(label)) {
				builder.setData(fixedLabel);
				builder.setRaw_data(label);
				documentStore.push(builder.getDocument());
			}
			builder.clear();
			rs_label.next();
		}

		// Clean up

		rs_label.close();

		log.info("Done Labels!");
	}
}

package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.MarkerAccIDLuceneDocBuilder;
import org.jax.mgi.searchtoolIndexer.util.InitCap;
import org.jax.mgi.searchtoolIndexer.util.ProviderHashMap;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This is responsible for gathering up and information we might need in
 * the MarkerAccID index.
 * 
 * @author mhall
 * 
 * @has An instance of IndexCfg, which is used to setup this object.
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts 
 * parsing through its result set. For each record, we generate a Lucene 
 * document, and place it on the shared stack.
 * 
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class MarkerAccIDGatherer extends DatabaseGatherer {

    // Class Variables

    private MarkerAccIDLuceneDocBuilder builder = 
        new MarkerAccIDLuceneDocBuilder();

    private ProviderHashMap phmg;
   
    /**
     * Create a new instance of the MarkerExactGatherer, and populate its
     * translation hash maps.
     * 
     * @param config
     */

    public MarkerAccIDGatherer(IndexCfg config) {

        super(config);

        phmg = new ProviderHashMap(config);

    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create the MarkerExact documents.
     *  
     */

    public void runLocal() throws Exception {
            doMarkerAccession();
            doAlleleAccession();
            doOrthologAccession();
            doESCellLineAccessionViaAllele();
    }

    /**
     * Grab accession ID's directly associated with markers.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doMarkerAccession() 
        throws SQLException, InterruptedException {

        // SQL for this Subsection

        log.info("Gathering Accession ID's for Markers");
        
        // Select all marker related accession id's, where the id is 
        // not private, where its for the mouse, and the marker has not
        // been withdrawn.
        
        String GENE_ACC_KEY = "SELECT a._Object_key, a.accID, a._LogicalDB_key"
                + " FROM dbo.ACC_Accession a,  MRK_Marker m"
                + " where private = 0 and _MGIType_key = 2 and"
                + " a._Object_key = m._Marker_key and m._Organism_key = 1"
                + " and m._Marker_Status_key != 2";

        // Gather the data

        ResultSet rs_acc = executor.executeMGD(GENE_ACC_KEY);
        rs_acc.next();

        String provider = "";
        
        log.info("Time taken to gather marker's accession id result set: "
                        + executor.getTiming());

        // Parse it

        while (!rs_acc.isAfterLast()) {

            builder.setData(rs_acc.getString("accID"));
            builder.setDb_key(rs_acc.getString("_Object_key"));
            builder.setDataType(IndexConstants.ACCESSION_ID);
            builder.setDisplay_type("ID");
            provider = phmg.get(rs_acc.getString("_LogicalDB_key"));
            
            // Set the provider, blanking it out if needed.
            
            if (!provider.equals("")) {
                builder.setProvider("(" + provider + ")");
            } else {
                builder.setProvider(provider);
            }
            
            // Place the document on the stack
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_acc.next();

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

    private void doAlleleAccession() 
        throws SQLException, InterruptedException {

        log.info("Gathering Accession ID's for Alleles");
        
        // Gather up all allele accession ID's and the markers they are 
        // related to, as long as the marker hasn't been withdrawn.

        String ALLELE_TO_MARKER_EXACT = "select al._Marker_key,  ac.accID"
                + " from ALL_Allele al, ACC_Accession ac, MRK_Marker m"
                + " where al._Marker_key != null and al._Allele_key" 
                + " = ac._Object_key and ac._MGIType_key = 11"
                + " and al._Marker_key = m._Marker_key"
                + " and m._Marker_Status_key !=2";

        // Gather the data

        ResultSet rs_all_acc = executor.executeMGD(ALLELE_TO_MARKER_EXACT);
        rs_all_acc.next();

        log.info("Time taken to gather Allele's Accession ID result set: "
                        + executor.getTiming());

        // Parse it

        while (!rs_all_acc.isAfterLast()) {
            builder.setData(rs_all_acc.getString("accID"));
            builder.setDb_key(rs_all_acc.getString("_Marker_key"));
            builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
            builder.setDisplay_type("Allele ID");
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_all_acc.next();
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

    private void doOrthologAccession() throws SQLException,
            InterruptedException {

        log.info("Gathering Accession ID's for Orthologs");
        
        // Gather up any accession ID, that can be directly related to
        // a mouse marker, via the MRK_Homology_cache.  We are directly
        // matching up ortholog's for known associations to mouse.

        String ORTH_TO_MARKER_ACC_ID = "select distinct mouse._Marker_key,"
                + " a.accID, a._LogicalDB_key, m.commonName "
                + "from MRK_Homology_Cache mouse, MRK_Homology_Cache nonmouse,"
                + " ACC_Accession a, MGI_Organism m, MRK_Marker mm"
                + " where mouse._Class_key = nonmouse._Class_key and"
                + " mouse._Organism_key = 1 and nonmouse._Organism_key != 1"
                + " and nonmouse._Marker_key = a._Object_key"
                + " and mouse._Marker_key = mm._Marker_key"
                + " and mm._Marker_Status_key !=2"
                + " and nonmouse._Organism_key = m._Organism_key"
                + " and a._MGIType_key = 2 and a.private = 0";

        // Gather the data

        ResultSet rs_orth_acc = executor.executeMGD(ORTH_TO_MARKER_ACC_ID);
        rs_orth_acc.next();

        log.info("Time taken to gather Ortholog's Accession ID result set: "
                        + executor.getTiming());

        // Parse it

        while (!rs_orth_acc.isAfterLast()) {

            builder.setData(rs_orth_acc.getString("accID"));
            builder.setDb_key(rs_orth_acc.getString("_Marker_key"));
            builder.setDataType(IndexConstants.ORTH_ACCESSION_ID);
            builder.setDisplay_type("ID");
            
            // Another special case for the provider hash map.  We must append
            // the organism's name for these cases.
            
            builder.setProvider("("+phmg.get(
                    rs_orth_acc.getString("_LogicalDB_key")) + " - " 
                    + InitCap.initCap(rs_orth_acc.getString("commonName"))+")");
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();

            rs_orth_acc.next();

        }

        // Clean up

        rs_orth_acc.close();
        log.info("Done Ortholog Accession ID's!");

    }

    /**
     * Grab ESCellLine Accession ID's that have been associated to Markers 
     * through Alleles.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */

    private void doESCellLineAccessionViaAllele() throws SQLException,
            InterruptedException {

        log.info("Gathering Accession ID's for ESCell Lines via Alleles");
        
        // Grab all es cell line id's whose alleles have been associated
        // to a non withdrawn marker.
        
        String ES_CELL_LINE_TO_MARKER_ACC_ID = "select al._Marker_key,"
                + " ac.accID, ac._LogicalDB_key"
                + " from ALL_Allele al, ACC_Accession ac, MRK_Marker m," 
                + " ALL_Allele_CellLine aac"
                + " where al._Marker_key != null" 
                + " and al._Allele_key = aac._Allele_key"
                + " and ac._MGIType_key = 28  and ac._Object_key ="
                + " aac._MutantCellLine_key and al._Marker_key ="
                + " m._Marker_key and m._Marker_Status_key !=2";

        // Gather the data

        log.info(ES_CELL_LINE_TO_MARKER_ACC_ID);
        
        ResultSet rs_es_acc_by_allele = executor.executeMGD(ES_CELL_LINE_TO_MARKER_ACC_ID);
        rs_es_acc_by_allele.next();

        log.info("Time taken to gather ES Cell Accession id result set: "
                        + executor.getTiming());

        // Parse it

        while (!rs_es_acc_by_allele.isAfterLast()) {
            builder.setData(rs_es_acc_by_allele.getString("accID"));
            builder.setDb_key(rs_es_acc_by_allele.getString("_Marker_key"));
            builder.setDataType(IndexConstants.ES_ACCESSION_ID);
            builder.setDisplay_type("Cell Line ID");
            builder.setProvider("("+phmg.get(rs_es_acc_by_allele
                    .getString("_LogicalDB_key"))+")");
            
            // Place the document on the stack.
            
            documentStore.push(builder.getDocument());
            builder.clear();
            rs_es_acc_by_allele.next();
        }

        // Clean up

        rs_es_acc_by_allele.close();
        log.info("Done ES Cell Line Accession ID's!");
    }
}

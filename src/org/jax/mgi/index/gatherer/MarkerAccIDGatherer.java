package org.jax.mgi.index.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jax.mgi.index.luceneDocBuilder.MarkerAccIDLuceneDocBuilder;
import org.jax.mgi.shr.config.IndexCfg;
import org.jax.mgi.shr.searchtool.IndexConstants;

/**
 * This class is responsible for gathering up and information we might need in
 * the MarkerAccID index.
 * 
 * @author mhall
 * 
 * @has An instance of the provider hash map gatherer, which translates from
 * logical Db's -> Display Strings
 * 
 * A single instance of the MarkerAccIDLuceneDocBuilder which is used 
 * throughout to create its needed Lucene documents
 * 
 * @does Upon being started, it begins gathering up its needed data components.
 * Each component basically makes a call to the database and then starts 
 * parsing through its result set. For each record, we generate a Lucene 
 * document, and place it on the shared stack.
 * 
 * After all of the components are finished, we notify the stack that gathering
 * is complete, clean up our jdbc connections and exit.
 */

public class MarkerAccIDGatherer extends AbstractGatherer {

    // Class Variables

    private MarkerAccIDLuceneDocBuilder maldb = 
        new MarkerAccIDLuceneDocBuilder();

    private ProviderHashMapGatherer phmg;
    
    private Logger  log = 
        Logger.getLogger(MarkerAccIDGatherer.class.getName());

    private Date    writeStart;
    private Date    writeEnd;

    /**
     * Create a new instance of the MarkerExactGatherer, and populate its
     * translation hash maps.
     * 
     * @param config
     */

    public MarkerAccIDGatherer(IndexCfg config) {

        super(config);

        phmg = new ProviderHashMapGatherer(config);

    }

    /**
     * This method encapsulates the algorithm used for gathering the data 
     * needed to create the MarkerExact documents.
     */

    public void run() {
        try {

            doMarkerAccession();

            doAlleleAccession();

            doOrthologAccession();

            doESCellLineAccessionViaAllele();
            
            doESCellLineAccessionViaMarkers();

        } catch (Exception e) {
            log.error(e);
        } finally {
            sis.setComplete();
            cleanup();
        }
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
        
        String GENE_ACC_KEY = "SELECT a._Object_key, a.accID, a._LogicalDB_key"
                + " FROM dbo.ACC_Accession a,  MRK_Marker m"
                + " where private = 0 and _MGIType_key = 2 and"
                + " a._Object_key = m._Marker_key and m._Organism_key = 1"
                + " and m._Marker_Status_key != 2"
                + " and accID not in (SELECT a.accID"
                + " FROM dbo.ACC_Accession a"
                + " where private = 0 and _MGIType_key = 28)";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_acc = execute(GENE_ACC_KEY);
        rs_acc.next();

        writeEnd = new Date();

        String provider = "";
        
        log.info(
                "Time taken to gather marker's accession id result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_acc.isAfterLast()) {

            maldb.setData(rs_acc.getString("accID"));
            maldb.setDb_key(rs_acc.getString("_Object_key"));
            maldb.setDataType(IndexConstants.ACCESSION_ID);
            maldb.setDisplay_type("ID");
            provider = phmg.get(rs_acc.getString("_LogicalDB_key"));
            
            // Set the provider, blanking it out if needed.
            
            if (!provider.equals("")) {
                maldb.setProvider("(" + provider + ")");
            } else {
                maldb.setProvider(provider);
            }
            sis.push(maldb.getDocument());
            maldb.clear();
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

    private void doAlleleAccession() throws SQLException, InterruptedException {

        log.info("Gathering Accession ID's for Alleles");
        
        // SQL for this Subsection

        String ALLELE_TO_MARKER_EXACT = "select al._Marker_key,  ac.accID"
                + " from ALL_Allele al, ACC_Accession ac, MRK_Marker m"
                + " where al._Marker_key != null and al._Allele_key" 
                + " = ac._Object_key and ac._MGIType_key = 11"
                + " and al._Marker_key = m._Marker_key"
                + " and m._Marker_Status_key !=2";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_all_acc = execute(ALLELE_TO_MARKER_EXACT);
        rs_all_acc.next();

        writeEnd = new Date();

        log.info("Time taken to gather Allele's Accession ID result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_all_acc.isAfterLast()) {
            maldb.setData(rs_all_acc.getString("accID"));
            maldb.setDb_key(rs_all_acc.getString("_Marker_key"));
            maldb.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
            maldb.setDisplay_type("Allele ID");
            sis.push(maldb.getDocument());
            maldb.clear();
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
        
        // Sql for this Subsection

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

        writeStart = new Date();

        ResultSet rs_orth_acc = execute(ORTH_TO_MARKER_ACC_ID);
        rs_orth_acc.next();

        writeEnd = new Date();

        log.info("Time taken to gather Ortholog's Accession ID result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_orth_acc.isAfterLast()) {

            maldb.setData(rs_orth_acc.getString("accID"));
            maldb.setDb_key(rs_orth_acc.getString("_Marker_key"));
            maldb.setDataType(IndexConstants.ORTH_ACCESSION_ID);
            maldb.setDisplay_type("ID");
            
            // Another special case for the provider hash map.  We must append
            // the organism's name for these cases.
            
            maldb.setProvider("("+phmg.get(
                    rs_orth_acc.getString("_LogicalDB_key")) + " - " 
                    + initCap(rs_orth_acc.getString("commonName"))+")");
            sis.push(maldb.getDocument());
            maldb.clear();

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
        
        // SQL for this Subsection

        String ES_CELL_LINE_TO_MARKER_ACC_ID = "select al._Marker_key,"
                + " ac.accID, ac._LogicalDB_key"
                + " from ALL_Allele al, ACC_Accession ac, MRK_Marker m"
                + " where al._Marker_key != null"
                + " and ac._MGIType_key = 28  and ac._Object_key ="
                + " al._MutantESCellLine_key and al._Marker_key ="
                + " m._Marker_key and m._Marker_Status_key !=2";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_es_acc = execute(ES_CELL_LINE_TO_MARKER_ACC_ID);
        rs_es_acc.next();

        writeEnd = new Date();

        log.info("Time taken to gather ES Cell Accession id result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_es_acc.isAfterLast()) {
            maldb.setData(rs_es_acc.getString("accID"));
            maldb.setDb_key(rs_es_acc.getString("_Marker_key"));
            maldb.setDataType(IndexConstants.ES_ACCESSION_ID);
            maldb.setDisplay_type("Cell Line ID");
            maldb.setProvider("("+phmg.get(rs_es_acc
                    .getString("_LogicalDB_key"))+")");
            sis.push(maldb.getDocument());
            maldb.clear();
            rs_es_acc.next();
        }

        // Clean up

        rs_es_acc.close();
        log.info("Done ES Cell Line Accession ID's!");
    }

    /**
     * Grab ESCellLine Accession ID's that have been associated to Markers via
     * a direct relationship to a marker record.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    
    private void doESCellLineAccessionViaMarkers() throws SQLException,
            InterruptedException {
    
        // SQL for this Subsection

        log.info("Gathering Accession ID's for ES Cell Lines Via Markers");
        
        // Grab any marker acc id records, whose acc id also directly matches
        // an es cell line accid record.
        
        String GENE_ACC_KEY = "SELECT a._Object_key, a.accID, a._LogicalDB_key"
                + " FROM dbo.ACC_Accession a,  MRK_Marker m"
                + " where private = 0 and _MGIType_key = 2 and"
                + " a._Object_key = m._Marker_key and m._Organism_key = 1"
                + " and m._Marker_Status_key != 2"
                + " and accID in (SELECT a.accID"
                + " FROM dbo.ACC_Accession a"
                + " where private = 0 and _MGIType_key = 28)";

        // Gather the data

        writeStart = new Date();

        ResultSet rs_acc = execute(GENE_ACC_KEY);
        rs_acc.next();

        writeEnd = new Date();

        String provider = "";
        
        log.info(
                "Time taken to gather marker's accession id result set: "
                        + (writeEnd.getTime() - writeStart.getTime()));

        // Parse it

        while (!rs_acc.isAfterLast()) {

            maldb.setData(rs_acc.getString("accID"));
            maldb.setDb_key(rs_acc.getString("_Object_key"));
            maldb.setDataType(IndexConstants.ES_ACCESSION_ID);
            maldb.setDisplay_type("Cell Line ID");
            provider = phmg.get(rs_acc.getString("_LogicalDB_key"));
            
            // Again, if we have a blank case, blank out the provider. 
            
            if (!provider.equals("")) {
                maldb.setProvider("(" + provider + ")");
            } else {
                maldb.setProvider(provider);
            }
            sis.push(maldb.getDocument());
            maldb.clear();
            rs_acc.next();

        }

        // Clean up

        rs_acc.close();
        log.info("Done Accession ID's for Markers!");
    }
}

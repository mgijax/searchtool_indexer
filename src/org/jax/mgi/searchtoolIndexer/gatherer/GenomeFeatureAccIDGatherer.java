package org.jax.mgi.searchtoolIndexer.gatherer;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jax.mgi.searchtoolIndexer.luceneDocBuilder.GenomeFeatureAccIDLuceneDocBuilder;
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

public class GenomeFeatureAccIDGatherer extends DatabaseGatherer {

    // Class Variables

    private GenomeFeatureAccIDLuceneDocBuilder builder =
        new GenomeFeatureAccIDLuceneDocBuilder();

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
     * This method encapsulates the algorithm used for gathering the data
     * needed to create the MarkerExact documents.
     *
     */

    public void runLocal() throws Exception {
            doMarkerAccession();
            doAlleleAccession();
            doOrthologAccession();
            doAllelesByESCellLines();
            doAllelesBySequence();
            doAllelesByESCellLineNames();
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
                + " and m._Marker_Status_key != 2 and m._Marker_Type_key != 12";

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
            builder.setObject_type("MARKER");
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

        String ALLELE_TO_MARKER_EXACT = "select al._allele_key,  ac.accID"
                                + " from ALL_Allele al, ACC_Accession ac"
                                + " where al._Allele_key = ac._Object_key and" 
                                + " ac._MGIType_key = 11 " 
                                + " and al.isWildType != 1";

        // Gather the data

        ResultSet rs_all_acc = executor.executeMGD(ALLELE_TO_MARKER_EXACT);
        rs_all_acc.next();

        log.info("Time taken to gather Allele's Accession ID result set: "
                        + executor.getTiming());

        // Parse it

        while (!rs_all_acc.isAfterLast()) {
            builder.setData(rs_all_acc.getString("accID"));
            builder.setDb_key(rs_all_acc.getString("_allele_key"));
            builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
            builder.setDisplay_type("Allele ID");
            builder.setObject_type("ALLELE");

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
                + " and a._MGIType_key = 2 and a.private = 0" 
                + " and mm._Marker_Type_key != 12";

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
            builder.setObject_type("MARKER");

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
     * Gather the ES Cell Line data.  Please note, we only gather 
     * Accession ID's for this data type if they have a direct relationship 
     * to an allele.
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
        rs_escell.next();

        log.info("Time taken gather es cell line data set: "
                + executor.getTiming());

        // Parse it
        
        while (!rs_escell.isAfterLast()) {
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
            rs_escell.next();
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

    private void doAllelesBySequence() throws SQLException, InterruptedException {

        // SQL for this Subsection

        // gather up the non private accession id's for alleles
        
        String OTHER_ALL_BY_SEQUENCE_SEARCH = "select distinct a._Accession_key, a.accID," +
                " saa._Allele_key, 'ALLELE' as _MGIType_key," +
                " a.preferred, a._LogicalDB_key" +
                " from ACC_Accession a, SEQ_Allele_Assoc saa"+
                " where a._MGIType_key = 19 and a.private != 1"+
                " and saa._Sequence_key = a._Object_key and saa._Allele_key != null";

        // Gather the data

        ResultSet rs_all = executor.executeMGD(OTHER_ALL_BY_SEQUENCE_SEARCH);
        rs_all.next();

        log.info("Time taken gather allele by sequence data set: "
                + executor.getTiming());

        // Parse it

        while (!rs_all.isAfterLast()) {

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
            rs_all.next();
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
    
        String OTHER_ES_CELL_LINE_SEARCH = "select aac._Allele_key, ac.cellLine " +
                "from all_cellline ac, ALL_Allele_CellLine aac " + 
                "where ac._CellLine_key not in (select distinct " + 
                "_Object_key from acc_accession where _MGIType_key = 28 " + 
                "and private != 1) and ac.cellLine != 'Not Specified' and " + 
                "ac.cellLine != 'Other (see notes)' and ac.isMutant = 1 and " + 
                "ac._CellLine_key = aac._MutantCellLine_key";
    
        // Gather the data
    
        ResultSet rs_escell = executor.executeMGD(OTHER_ES_CELL_LINE_SEARCH);
        rs_escell.next();
    
        log.info("Time taken gather es cell line name data set: "
                + executor.getTiming());
    
        // Parse it
        
        while (!rs_escell.isAfterLast()) {
            builder.setData(rs_escell.getString("cellLine"));
            builder.setDb_key(rs_escell.getString("_Allele_key"));
            builder.setDataType(IndexConstants.ALLELE_ACCESSION_ID);
            builder.setDisplay_type("Cell Line");
            builder.setObject_type("ALLELE");
    
            documentStore.push(builder.getDocument());
            
            builder.clear();
            rs_escell.next();
        }
    
        // Clean up
        
        log.info("Done creating documents for es cell lines!");
        rs_escell.close();
    }
    
}
package org.jax.mgi.searchtoolIndexer.gatherer;

public class Location {

    private String chromosome;
    private String start_coordinate;
    private String end_coordinate;
    private String strand;
    private String source;
    
    public String getChromosome() {
        return chromosome;
    }
    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }
    public String getStart_coordinate() {
        return start_coordinate;
    }
    public void setStart_coordinate(String startCoordinate) {
        start_coordinate = startCoordinate;
    }
    public String getEnd_coordinate() {
        return end_coordinate;
    }
    public void setEnd_coordinate(String endCoordinate) {
        end_coordinate = endCoordinate;
    }
    public String getStrand() {
        return strand;
    }
    public void setStrand(String strand) {
        this.strand = strand;
    }
    public String getSource() {
        return source;
    }
    public void setSource(String source) {
        this.source = source;
    }
    
}

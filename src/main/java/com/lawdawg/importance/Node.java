package com.lawdawg.importance;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;

@JsonSerialize(include = Inclusion.NON_NULL)
public class Node {
    
    public String id  = null;

    @JsonProperty("p")
    public Double pagerank = null;
    
    @JsonProperty("s")
    public Double sumOfPartials = null;

    public List<String > edges = null;
    public List<Double> weights = null;

}

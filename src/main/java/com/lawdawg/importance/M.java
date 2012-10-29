package com.lawdawg.importance;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class M implements Mapper<LongWritable, Text, Text, Text> {
    
    private final Text key = new Text();
    private final Text value = new Text();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private double alpha;
    private double lost;
    
    private static double percentDifference(double a, double b) {
        return Math.abs(2 * (a - b) / (a + b));
    }
    
    @Override
    public void configure(JobConf conf) {
        this.lost = Double.parseDouble(conf.get("lost"));
        this.alpha = Double.parseDouble(conf.get("alpha"));
    }

    @Override
    public void close() throws IOException {
        
    }

    private double pr(final double sum) {
        return this.alpha + (1 - this.alpha) * (sum + this.lost);
    }
    
    @Override
    public void map(final LongWritable ignored, final Text json, final OutputCollector<Text, Text> collector,
            Reporter reporter) throws IOException {

        Node node = objectMapper.readValue(json.toString(), Node.class);

        boolean bootstrap = node.pagerank == null;
        
        double previousPagerank = bootstrap ? 0.0 : node.pagerank;
        double nextPagerank = bootstrap ? 1.0 : this.pr(node.sumOfPartials);

        if (node.edges != null) {
            double size = node.edges.size();
            for (int i = 0; i < size; i++) {
                String destination = node.edges.get(i);
                double weight = (node.weights == null ? (1.0 / size) : node.weights.get(i));
                double partial = nextPagerank * weight;
                collect(collector, destination, partial);
            }
        }

        // re-emit this node
        node.pagerank = nextPagerank;
        node.sumOfPartials = null;
        collect(collector, node);
        
        // increment counters
        if (!bootstrap) {
            double pd = percentDifference(previousPagerank, nextPagerank);
            Deltas.increment(reporter, pd);
        }
    }
    
    private Node partialNode = new Node();
    private void collect(OutputCollector collector, String destination, double partial) throws IOException {
        partialNode.sumOfPartials = partial;
        this.key.set(destination);
        this.value.set(objectMapper.writeValueAsString(partialNode));
        collector.collect(this.key, this.value);
    }
    private void collect(OutputCollector collector, Node node) throws IOException {
        key.set(node.id);
        value.set(objectMapper.writeValueAsString(node));
        collector.collect(this.key, this.value);
    }
    
    enum Deltas {
        D1(1.0),
        D10(0.1),
        D100(0.01),
        D1000(0.001),
        D10000(0.0001),
        D100000(0.00001);
        
        Deltas(double d) {
            this.limit = d;
        }
        
        private double limit;
        private static void increment(Reporter r, double pd) {
            for (Deltas d : Deltas.values()) {
                if (pd < d.limit) {
                    r.incrCounter(d, 1);
                }
            }
        }
    }
}

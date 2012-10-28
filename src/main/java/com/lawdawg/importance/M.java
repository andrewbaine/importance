package com.lawdawg.importance;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

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

    @Override
    public void map(final LongWritable ignored, final Text json, final OutputCollector<Text, Text> collector,
            Reporter reporter) throws IOException {
        JsonNode node = objectMapper.readTree(json.toString());
        ObjectNode objectNode = objectMapper.createObjectNode();

        String id = node.get("id").asText();
        objectNode.put("id", id);

        key.set(id);
        
        double last = node.has("p") ? node.get("p").asDouble() : 0.0;
        double sum = node.has("sum") ? node.get("sum").asDouble() : 1.0;        
        double p = alpha + (1 - alpha) * (sum + this.lost);
        objectNode.put("p", p);
        
        if (node.has("edges") && node.get("edges").size() > 0) {
            JsonNode edges = node.get("edges");
            objectNode.put("edges", edges);
            double denominator = edges.size();

            JsonNode weights = null;
            if (node.has("weights")) {
                weights = node.get("weights");
                objectNode.put("weights", weights);
            }
            
            // distribute p among edges
            for (int i = 0; i < edges.size(); i++) {
                String destination = edges.get(i).asText();
                double weight = (weights == null ? (1.0 / denominator) : weights.get(i).asDouble());
                double partial = p * weight;
                String js = String.format("{ \"partial\": %.5f }", partial);
                this.collect(collector, destination, js);
            }
        }

        String value = objectMapper.writeValueAsString(objectNode);
        this.collect(collector, id, value);
        
        double pd = percentDifference(last, p);
        Deltas.increment(reporter, pd);
    }
    
    private void collect(OutputCollector out, String key, String value) throws IOException {
        this.key.set(key);
        this.value.set(value);
        out.collect(this.key, this.value);
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

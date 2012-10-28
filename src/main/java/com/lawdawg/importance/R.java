package com.lawdawg.importance;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class R implements Reducer<Text, Text, NullWritable, Text> {

    private final NullWritable NULL = NullWritable.get();
    private final Text value = new Text();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private boolean last;
    
    @Override
    public void configure(JobConf conf) {
        this.last = conf.getBoolean("last", false);
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void reduce(Text id, Iterator<Text> values,
            OutputCollector<NullWritable, Text> collector, Reporter reporter) throws IOException {
        ObjectNode value = objectMapper.createObjectNode();
        value.put("id", id.toString());
        value.put("p", 0.0);
        double sum = 0.0;
        while (values.hasNext()) {
            ObjectNode node = (ObjectNode)objectMapper.readTree(values.next().toString());
            if (node.has("partial")) {
                sum += node.get("partial").asDouble();
            } else {
                if (node.has("edges")) {
                    value.put("edges", node.get("edges"));
                }
                if (node.has("weights")) {
                    value.put("weights", node.get("weights"));
                }
                value.put("p", node.get("p"));
            }
        }
        value.put("sum", sum);
        if (last) {
            value.remove("edges");
            value.remove("weights");
        }
        this.value.set(objectMapper.writeValueAsString(value));

        collector.collect(NULL, this.value);
        reporter.incrCounter(Counters.G, 1L);
        reporter.incrCounter(Counters.CONSERVED, (long)(sum * Main.MULTIPLIER));
    }

    public enum Counters {
        G,
        CONSERVED
    }
}

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
    }

    @Override
    public void reduce(Text id, Iterator<Text> values,
            OutputCollector<NullWritable, Text> collector, Reporter reporter) throws IOException {
        Node n = new Node();
        n.id = id.toString();

        double sum = 0.0;
        while (values.hasNext()) {
            Node node = objectMapper.readValue(values.next().toString(), Node.class);
            if (node.sumOfPartials == null) {
                n = node;
            } else {
                sum += node.sumOfPartials;
            }
        }
        n.sumOfPartials = sum;
        if (this.last){
            n.edges = null;
            n.weights = null;
            n.sumOfPartials = null;
        }
        this.value.set(objectMapper.writeValueAsString(n));
        collector.collect(NULL, this.value);
        reporter.incrCounter(Counters.G, 1L);
        reporter.incrCounter(Counters.CONSERVED, (long)(sum * Main.MULTIPLIER));
    }

    public enum Counters {
        G,
        CONSERVED
    }
}

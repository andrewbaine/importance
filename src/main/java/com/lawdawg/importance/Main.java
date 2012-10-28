package com.lawdawg.importance;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lawdawg.importance.R.Counters;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static final double MULTIPLIER = 1000000.0;

    private static Path mkTmpDir() throws IOException {
        String tmp = UUID.randomUUID().toString();
        JobConf conf = new JobConf();
        Path result = new Path(tmp);
        FileSystem.get(conf).mkdirs(new Path(tmp));
        return result;
    }

    private static class Conf {
        
        public Conf(double alpha, double lost, boolean last) {
            this.alpha = alpha;
            this.lost = lost;
            this.last = last;
        }
        
        public final double alpha;
        public final double lost;
        public final boolean last;
    }
    
    private static Conf run(Path in, Path out, Conf c) throws IOException {
        JobConf conf = new JobConf();
        conf.set("lost", String.format("%.10f", c.lost));
        conf.set("alpha", String.format("%.10f", c.alpha));
        conf.setBoolean("last", c.last);
        
        conf.setMapperClass(M.class);
        conf.setReducerClass(R.class);
        
        FileInputFormat.setInputPaths(conf, in);
        FileOutputFormat.setOutputPath(conf, out);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        RunningJob job = JobClient.runJob(conf);
        double g = job.getCounters().getCounter(Counters.G);
        double conserved = job.getCounters().getCounter(Counters.CONSERVED);
        
        double lost = (g * MULTIPLIER - conserved) / (g * MULTIPLIER);
        return new Conf(c.alpha, lost, false);
    }
    
    public static void main(String[] args) throws IOException, ParseException {
        
        Option inputOption = OptionBuilder.withArgName("path")
                .hasArg()
                .withDescription("input path").create("input");
        
        Option outputOption = OptionBuilder.withArgName("path")
                .hasArg()
                .withDescription("output path").create("output");
        
        Option alphaOption = OptionBuilder.withArgName("alpha")
                .hasArg()
                .withDescription("pr = alpha + (1 - alpha) * (sum of inbound partials)")
                .create("alpha");
        
        Option iterationsOption = OptionBuilder.withArgName("n").hasArg()
                .withDescription("number of iterations").create("iterations");
        
        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(alphaOption);
        options.addOption(iterationsOption);
        
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        
        Path inputPath = new Path(commandLine.getOptionValue("input"));
        Path outputPath = new Path(commandLine.getOptionValue("output"));
        double alpha = Double.parseDouble(commandLine.getOptionValue("alpha"));
        int iterations = Integer.parseInt(commandLine.getOptionValue("iterations"));
        
        Path tmp = mkTmpDir();
        JobConf conf = new JobConf();
        
        Conf c = new Conf(alpha, 0.0, false);
        for (int i = 0; i < iterations; i++) {
            Path tmpOutputPath = new Path(tmp, String.format("tmp%03d", i));
            c = run(inputPath, tmpOutputPath, c);
            inputPath = tmpOutputPath;
        }
        c = new Conf(c.alpha, c.lost, true);
        c = run(inputPath, outputPath, c);
        FileSystem.get(conf).delete(tmp, true);
    }

}

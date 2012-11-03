package com.lawdawg.importance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test
public class ImportanceIT {

    private File outputDirectory = null;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final double DELTA = 0.0001;

    @BeforeClass
    public void setup() {
        String tmp = UUID.randomUUID().toString();
        this.outputDirectory = new File(tmp);
        outputDirectory.mkdirs();
    }
    
    @AfterClass
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(this.outputDirectory);
    }

    private static void assertMapsEqual(Map<String, Double> actual, Map<String, Double> expected) {
        Assert.assertEquals(actual.size(), expected.size());
        for (Map.Entry<String, Double>  entry : expected.entrySet()) {
            double expectedP = entry.getValue();
            double actualP = actual.get(entry.getKey());
            Assert.assertEquals(actualP, expectedP, DELTA);
        }
    }
    
    private Map<String, Double> parseFile(final File file) throws JsonProcessingException, IOException {
        Map<String, Double> map = new HashMap<String, Double>();
        parse(file, map);
        return map;
    }
    
    private void parse(final File file, final Map<String, Double> map) throws JsonProcessingException, IOException {
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            String json = scanner.nextLine();
            JsonNode node = objectMapper.readTree(json.toString());
            map.put(node.get("id").asText(), node.get("p").asDouble());
        }        
    }
    
    private Map<String, Double> parseDirectory(final String dir) throws JsonProcessingException, IOException {
        Map<String, Double> map = new HashMap<String, Double>();
        for (String filename : new File(dir).list()) {
            if (!"_SUCCESS".equals(filename) && !filename.startsWith(".")) {
                parse(new File(dir, filename), map);
            }
        }
        return map;
    }

    @DataProvider(name="listJobsProvider")
    private Object[][] listJobs() throws URISyntaxException, IOException {
        URL url = getClass().getResource("tests");
        File dir = new File(url.toURI());
        File[] files = dir.listFiles();
        Object[][] result = new Object[files.length][];
        int i = 0;
        for (File f : files) {
            String inputFile = new File(f, "input").getCanonicalPath();
            String expected = new File(f, "expected.json").getCanonicalPath();
            JsonNode node = this.objectMapper.readTree(new File(f, "config.json"));
            Integer iterations = node.get("iterations").asInt();
            Double alpha = node.get("alpha").asDouble();
            result[i++] = new Object[] { inputFile, expected, new Double(alpha), new Integer(iterations) };
        }
        return result;
    }
    
    @Test(dataProvider="listJobs", groups={"integration"})
    public void testJob(String inputDirectory, String expectedFile, Double alpha, Integer iterations) throws IOException, ParseException {
        File dir = new File(this.outputDirectory, inputDirectory).getParentFile();
        dir.mkdirs();
        String outputDirectory = new File(dir, "output").getCanonicalPath();
        String[] args = new String[] {
            "--input", inputDirectory,
            "--output", outputDirectory,
            "--iterations", Integer.toString(iterations),
            "--alpha", Double.toString(alpha)
        };
        RunImportanceJob.main(args);
        Map<String, Double> actual = parseDirectory(outputDirectory);
        Map<String, Double> expected = parseFile(new File(expectedFile));

        assertMapsEqual(actual, expected);
    }

}

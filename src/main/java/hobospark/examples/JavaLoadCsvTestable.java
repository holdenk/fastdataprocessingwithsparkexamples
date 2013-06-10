package hobospark.examples;

import spark.Accumulator;
import spark.api.java.JavaRDD;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaDoubleRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.DoubleFunction;
import spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVReader;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class JavaLoadCsvTestable {
    public static class ParseLine extends Function<String, Integer[]> {
	public Integer[] call(String line) throws Exception {
	    CSVReader reader = new CSVReader(new StringReader(line));
	    String[] parsedLine = reader.readNext();
	    Integer[] intLine = new Integer[parsedLine.length];
	    for (int i = 0; i < parsedLine.length; i++) {
		intLine[i] = Integer.parseInt(parsedLine[i]);
	    }
	    return intLine;
	}
    }
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
	System.err.println("Usage: JavaLoadCsvCounters <master> <inputfile>");
	System.exit(1);
    }
    String master = args[0];
    String inputFile = args[1];
    JavaSparkContext sc = new JavaSparkContext(master, "java load csv with counters",
					       System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<String> inFile = sc.textFile(inputFile);
    JavaRDD<Integer[]> splitLines = inFile.map(new ParseLine());
    splitLines.cache();
    System.out.println("Loaded data "+splitLines.collect());
    JavaDoubleRDD summedData = splitLines.map(new DoubleFunction<Integer[]>() {
	    public Double call(Integer[] in) {
		Double ret = 0.;
		for (int i = 0; i < in.length; i++) {
		    ret += in[i];
		}
		return ret;
	    }
	}
	);
    System.out.println(summedData.stats());
  }
}
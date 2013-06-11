package pandaspark.examples;

import spark.Accumulator;
import spark.api.java.JavaRDD;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaDoubleRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.DoubleFunction;
import spark.api.java.function.FlatMapFunction;

import au.com.bytecode.opencsv.CSVReader;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class JavaLoadCsvCounters {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
	System.err.println("Usage: JavaLoadCsvCounters <master> <inputfile>");
	System.exit(1);
    }
    String master = args[0];
    String inputFile = args[1];
    JavaSparkContext sc = new JavaSparkContext(master, "java load csv with counters",
        System.getenv("SPARK_HOME"), System.getenv("JARS"));
    final Accumulator<Integer> errors = sc.accumulator(0);
    JavaRDD<String> inFile = sc.textFile(inputFile);
    JavaRDD<Integer[]> splitLines = inFile.flatMap(new FlatMapFunction<String, Integer[]> (){
	    public Iterable<Integer[]> call(String line) {
		ArrayList<Integer[]> result = new ArrayList<Integer[]>();
		try {
		    CSVReader reader = new CSVReader(new StringReader(line));
		    String[] parsedLine = reader.readNext();
		    Integer[] intLine = new Integer[parsedLine.length];
		    for (int i = 0; i < parsedLine.length; i++) {
			intLine[i] = Integer.parseInt(parsedLine[i]);
		    }
		    result.add(intLine);
		} catch (Exception e) {
		    errors.add(1);
		}
		return result;
	    }
	}
	);
    splitLines.cache();
    System.out.println("Loaded data "+splitLines.collect());
    System.out.println("Error count "+errors.value());
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
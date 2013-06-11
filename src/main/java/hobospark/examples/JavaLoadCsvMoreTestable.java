package pandaspark.examples;

import spark.Accumulator;
import spark.api.java.JavaRDD;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaDoubleRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.DoubleFunction;
import spark.api.java.function.Function;
import spark.api.java.function.FlatMapFunction;

import au.com.bytecode.opencsv.CSVReader;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class JavaLoadCsvMoreTestable {
    public static class ParseLineWithAcc extends FlatMapFunction<String, Integer[]> {
	Accumulator<Integer> acc;
	ParseLineWithAcc(Accumulator<Integer> acc) {
	    this.acc = acc;
	}
	public Iterable<Integer[]> call(String line) throws Exception {
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
		acc.add(1);
	    }
	    return result;
	}
    }
    public static JavaDoubleRDD processData(Accumulator<Integer> acc, JavaRDD<String> input) {
	JavaRDD<Integer[]> splitLines = input.flatMap(new ParseLineWithAcc(acc));
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
	return summedData;
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
	JavaDoubleRDD summedData = processData(sc.accumulator(0), inFile);
	System.out.println(summedData.stats());
    }
}
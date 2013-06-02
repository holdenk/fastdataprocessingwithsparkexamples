package hobospark.examples;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;

public class JavaToy {
  public static void main(String[] args) throws Exception {
      JavaSparkContext sc = new JavaSparkContext(args[0], "java toy",
        System.getenv("SPARK_HOME"), System.getenv("JARS"));
      List<Integer> data = Arrays.asList(1,2,4);
      JavaRDD<Integer> dataRDD = sc.parallelize(data);
  }
}
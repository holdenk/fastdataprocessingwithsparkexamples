package hobospark.examples;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;

public class JavaSequenceLoad {
    public class TestPanda {
    }
  public static void main(String[] args) throws Exception {
      JavaSparkContext sc = new JavaSparkContext(args[0], "sequence load",
        System.getenv("SPARK_HOME"), System.getenv("JARS"));
      String file = args[1];
      JavaPairRDD<Text, IntWritable> dataRDD = sc.sequenceFile(file, Text.class, IntWritable.class);
      JavaPairRDD<String, Integer> cleanData = dataRDD.map(new PairFunction<Tuple2<Text, IntWritable>, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> pair) {
	  return new Tuple2<String, Integer>(pair._1().toString(), pair._2().get());
      }
    });
  }
}
package pandaspark.examples;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.*;

import java.util.Arrays;
import java.util.List;

public class JavaToy {
  public static void main(String[] args) throws Exception {
      JavaSparkContext sc = new JavaSparkContext(args[0], "java toy",
        System.getenv("SPARK_HOME"), System.getenv("JARS"));
      List<Integer> data = Arrays.asList(1,2,4,1,1,1,2,3);
      JavaRDD<Integer> dataRDD = sc.parallelize(data);
      JavaRDD<Integer> addedOne = dataRDD.map(new Function<Integer, Integer>() { public Integer call(Integer x) { return x+1;}});
      System.out.println(addedOne.collect());
      Integer summed = dataRDD.reduce(new Function2<Integer, Integer, Integer>(){ public Integer call(Integer x, Integer y) { return x+y;} });
      System.out.println(summed);
      JavaPairRDD<Integer, Integer> pairData = dataRDD.keyBy(new Function<Integer, Integer>(){ public Integer call(Integer x) { return x;}});
      JavaPairRDD<Integer, Integer> summedByKey = pairData.groupByKey().mapValues(new Function<List<Integer>, Integer >(){
	      public Integer call(List<Integer> x) {
		  Integer sum = 0;
		  for (Integer i : x) {
		      sum += i;
		  }
		  return sum;
	      }
	  }
	  );
      JavaPairRDD<Integer,Integer> summedByReduce = pairData.reduceByKey(new Function2<Integer, Integer, Integer>() { public Integer call (Integer a, Integer b){ return a+b; }});
      JavaPairRDD<Integer,Integer> summedByFold = pairData.foldByKey(0,
								new Function2<Integer, Integer, Integer>() {
								    @Override
								    public Integer call(Integer a, Integer b) {
									return a + b;
								    }
								});
      System.out.println("Summed by group by key and map values "+ summedByKey.collect());
      System.out.println("Summed by reduce " + summedByReduce.collect());
  }
}
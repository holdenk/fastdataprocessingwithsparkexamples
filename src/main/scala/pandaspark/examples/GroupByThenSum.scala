package pandaspark.examples

import scala.collection._
import scala.util._

import spark.SparkContext
import SparkContext._
import java.util.Random

object GroupByThenSum {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: GroupByThenSum <master>")
      System.exit(1)
    }
    
    var numMappers = 2
    var numKVPairs = 10000

    val sc = new SparkContext(args(0), "GroupByThenSum",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Int)](numKVPairs)
      for (i <- 0 until numKVPairs) {
        arr1(i) = (ranGen.nextInt(20), ranGen.nextInt(100))
      }
      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count
    val pandas = pairs1.groupByKey().map(x => (x._1, x._2.sum))
    val otherPandas = pairs1.reduceByKey((x,y) => x+y)
    val foldExample = pairs1.groupByKey().mapValues(x => {x.fold(0)((a,b) => a+b)})
    val coGroupedPandas = pandas.cogroup(otherPandas)
    pandas.cache()
    otherPandas.cache()
    coGroupedPandas.cache()


    println("Group by then sum:"+pandas.collect().toList)
    println("Reduce by key:"+otherPandas.collect().toList)
    println("co grouped:"+coGroupedPandas.collect().toList)

    System.exit(0)
  }
}


package pandaspark.examples

import spark._
import spark.SparkContext._
import spark.api.java.JavaSparkContext
import spark.api.java.JavaRDD
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class JavaLoadCsvMoreTestableSuite extends FunSuite with ShouldMatchers {
  test("sum data on input") {
    val sc = new JavaSparkContext("local", "Load Java CSV test")
    val counter: Accumulator[Integer] = sc.intAccumulator(0)
    val input: JavaRDD[String] = sc.parallelize(List("1,2","1,3","murh"))
    val javaLoadCsvMoreTestable = new JavaLoadCsvMoreTestable();
    val resultRDD = JavaLoadCsvMoreTestable.processData(counter,input)
    resultRDD.cache();
    val resultCount = resultRDD.count()
    val result = resultRDD.collect().toArray()
    resultCount should equal (2)
    result should equal (Array[Double](3.0, 4.0))
    counter.value should equal (1)
    sc.stop()
  }
}

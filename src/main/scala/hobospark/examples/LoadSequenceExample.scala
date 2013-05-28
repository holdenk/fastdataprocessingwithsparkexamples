package spark.examples

import spark.SparkContext
import spark.SparkContext._

object LoadSequenceExample {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: Sequence <master> <inputfile> <outputFile>")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "Load Sequence Example",
			      System.getenv("SPARK_HOME"),
			      Seq(System.getenv("JARS")))
    val data = sc.sequenceFile[String, Int](inputFile)
    val concreteData = data.collect();
    println(concreteData.mkString("\n"))
  }

}

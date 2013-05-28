package spark.examples

import spark.SparkContext
import spark.SparkContext._
import spark.SparkFiles;

import au.com.bytecode.opencsv.CSVReader

import java.io.StringReader

object LoadCsvWithCountersExample {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: LoadCsvExample <master> <inputfile>")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "Load CSV With Counters Example",
			      System.getenv("SPARK_HOME"),
			      Seq(System.getenv("JARS")))
    val invalidLineCounter = sc.accumulator(0)
    val invalidNumericLineCounter = sc.accumulator(0)
    sc.addFile(inputFile)
    val inFile = sc.textFile(inputFile)
    val splitLines = inFile.map(line => {
      try {
	val reader = new CSVReader(new StringReader(line))
	reader.readNext()
      } catch {
	case _ => invalidLineCounter += 1
      }
    }
			      )
    val numericData = splitLines.map(line => {
      try {
	line.map(_.toDouble)
      } catch {
	case _ => invalidNumericLineCounter += 1
      }
    }
    )
    val summedData = numericData.map(row => row.sum)
    println(summedData.collect().mkString(","))
    println("Errors: "+invalidLineCounter+","+invalidNumericLineCounter)
  }

}

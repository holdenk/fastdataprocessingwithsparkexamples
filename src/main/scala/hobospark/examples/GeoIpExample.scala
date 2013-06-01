package spark.examples

import spark.SparkContext
import spark.SparkContext._
import spark.SparkFiles;
import spark.util.Vector

import au.com.bytecode.opencsv.CSVReader

import java.io.StringReader

import com.snowplowanalytics.maxmind.geoip.IpGeo

case class DataPoint(x: Vector, y: Double)

object GeoIpExample {
  
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: GeoIpExample <master> <inputfile>")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val iterations = 100
    val sc = new SparkContext(master, "Load CSV With Counters Example",
			      System.getenv("SPARK_HOME"),
			      Seq(System.getenv("JARS")))
    val invalidLineCounter = sc.accumulator(0)
    sc.addFile(inputFile)
    val inFile = sc.textFile(inputFile)
    val parsedInput = inFile.flatMap(line => {
      try {
	val row = (new CSVReader(new StringReader(line))).readNext()
	Some((row(0),row.drop(1).map(_.toDouble)))
      } catch {
	case _ => {
	  invalidLineCounter += 1
	  None
	}
      }
    })
    val geoFile = sc.addFile("/opt/maxmind/GeoLiteCity.dat")
    val ipGeo = IpGeo(dbFile = SparkFiles.get("/opt/maxmind/GeoLiteCity.dat"))
    // getLocation gives back an option so we use flatMap to only output if its a some type
    val ipCountries = parsedInput.flatMap(pair => ipGeo.getLocation(pair._1).map(c => (pair._1, c.countryCode)))
    ipCountries.cache()
    val countries = ipCountries.values.distinct().collect()
    val countriesSignal = ipCountries.mapValues(country => countries.map(s => if (country == s) 1. else 0.))
    val dataPoints = parsedInput.join(countriesSignal).map(input => {
      input._2 match {
	case (countryData, originalData) => DataPoint(new Vector(countryData++originalData.slice(1,originalData.size-2)) , originalData(originalData.size-1))
      }
    })
    println("Data points :"+dataPoints)
  }
}

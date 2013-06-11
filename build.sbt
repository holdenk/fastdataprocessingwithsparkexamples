import AssemblyKeys._

assemblySettings

scalaVersion := "2.9.3"

name := "fastdataprocessingwithsparkexamples"

parallelExecution in Test := false

libraryDependencies ++= Seq(
    "org.spark-project" % "spark-core_2.9.3" % "0.7.2",
    "net.sf.opencsv" % "opencsv" % "2.0",
    "org.apache.hbase" % "hbase" % "0.94.6",
    "org.slf4j" % "slf4j-log4j12" % "1.6.1",
    "com.snowplowanalytics"  %% "scala-maxmind-geoip"  % "0.0.5",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "junit" % "junit" % "4.11" % "test",
    "com.novocode" % "junit-interface" % "0.8" % "test->default"
)

resolvers ++= Seq(
   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
   "SnowPlow Repo" at "http://maven.snplow.com/releases/",
   "Twitter Maven Repo" at "http://maven.twttr.com/"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
package hobospark.examples;

import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;

public class JavaHBaseLoad {
  public static void main(String[] args) throws Exception {
     JavaSparkContext sc = new JavaSparkContext(args[0], "sequence load",
        System.getenv("SPARK_HOME"), System.getenv("JARS"));
     Configuration conf = HBaseConfiguration.create();

    // Other options for configuring scan behavior are available. More information available at 
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html

     conf.set(TableInputFormat.INPUT_TABLE, args[1]);

    // Initialize hBase table if necessary
     HBaseAdmin admin = new HBaseAdmin(conf);
    if(!admin.isTableAvailable(args[1])) {
	HTableDescriptor tableDesc = new HTableDescriptor(args[1]);
	admin.createTable(tableDesc);
    }

    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(
        conf,
	TableInputFormat.class, 
	ImmutableBytesWritable.class,
	Result.class);

    System.exit(0);
  }
}

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import java.lang.System
import java.lang.ClassNotFoundException
import org.apache.hadoop.mapreduce.JobContext
import scala.runtime.ScalaRunTime._
import java.util.Arrays.sort
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/** word count in Spark **/

object SparkExample {
  def main(args: Array[String]) {

	val sc= new SparkContext("spark://PREFIX_NAME-m:7077", "WordCount")   // Replace PREFIX_NAME 

	val file = "word_count/romeo_juliet.txt"
	
	//SparkContext.textFile: reads file as a collection of lines
	val linesRDD = sc.textFile(file)
	
	// word count
	val wordsRDD = linesRDD.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_).sortByKey()
	val count = wordsRDD.count()
	val keysRDD = wordsRDD.keys
	println("word count = "+count)
	
	//get hbase table
        val conf = HBaseConfiguration.create()
        conf.set("google.bigtable.project.id", "sduskis-hello-shakespear");
        conf.set("google.bigtable.cluster.name", "us-central1-b");
        conf.set("google.bigtable.zone.name", "warmup-spark");
	conf.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");

        val tableName = "output-table"
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
	val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result]) //.sortByKey()
	val hBaseKeysRDD = hBaseRDD.keys.map(a => Bytes.toString(a.get()))

        val diff = keysRDD.toArray().toSet.diff(hBaseKeysRDD.toArray().toSet)
	println("Printing diff: ")
	diff.foreach(println)
	System.exit(0)

  }
}
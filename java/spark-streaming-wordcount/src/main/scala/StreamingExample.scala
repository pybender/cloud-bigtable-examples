import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary in Spark 1.3+
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table       
import org.apache.hadoop.hbase.client
import java.lang.System
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Increment
import scala.collection.JavaConversions._

object SparkExample {  
    
    def createConnection = {
       val confHBase = HBaseConfiguration.create()
       confHBase.set("google.bigtable.project.id", "PROJECT_ID");
       confHBase.set("google.bigtable.cluster.name", "CLUSTER_NAME");
       confHBase.set("google.bigtable.zone.name", "ZONE_NAME");
       confHBase.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");
       confHBase.set("spark.executor.extraJavaOptions", " -Xbootclasspath/p=/home/hadoop/alpn-boot-7.0.0.v20140317.jar")
       val conn = ConnectionFactory.createConnection(confHBase); 
       conn
    }

    def main(args: Array[String]) {

       val conf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount") 
       val ssc = new StreamingContext(conf, Seconds(30)) 
       val name = "test-output"
       val tableName = TableName.valueOf(name)
       val conn = createConnection
   
       try {
         val admin = conn.getAdmin()
	 if (!admin.tableExists(tableName)) {
	    val tableDescriptor = new HTableDescriptor(tableName)
	    tableDescriptor.addFamily(new HColumnDescriptor("WordCount"))
	    admin.createTable(tableDescriptor) 
	 }
	 admin.close()
       } catch {
         case e: Exception => e.printStackTrace
       }
       conn.close()

       val filePath = "word_count/"
       val dStream = ssc.textFileStream(filePath)

       def toBytes(word: String):Array[Byte] = {
         word.toCharArray.map(_.toByte)
       }

       dStream.foreachRDD { rdd => 
         rdd.foreachPartition {  partitionRecords =>
	    val conn1 = createConnection
            val tableName1 = TableName.valueOf(name)
	    val mutator = conn1.getBufferedMutator(tableName1)
	    try {
              partitionRecords.foreach{ line => 
                mutator.mutate(line.split(" ").filter(_!="").map(word => 
                new Increment(toBytes(word))
                .addColumn(toBytes("WordCount"), toBytes("Count"), 1L)).toList)
              }
            } catch {
              case retries_e: RetriesExhaustedWithDetailsException => retries_e.getCauses().foreach(_.printStackTrace)
              case e: Exception => e.printStackTrace
            }
            try {
              mutator.close()
            } catch {
              case retries_e: RetriesExhaustedWithDetailsException => retries_e.getCauses().foreach(_.printStackTrace)
              case e: Exception => e.printStackTrace
            }
	    conn1.close()
	 }
       }

       ssc.start()
       ssc.awaitTermination()
   }
}

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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
import java.lang.Exception
import java.lang.Runtime._

object SparkExample {  
    
    val COLUMN_FAMILY = "WordCount"
    val COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY)
    val COLUMN_NAME_BYTES = Bytes.toBytes("Count")

    def main(args: Array[String]) {
       if (args.length < 2) {
	   throw new Exception("Please enter input directory, and output table name as arguments")
       }
       val inputDirectory = args(0)
       val name = args(1)

       val conf = new SparkConf().setMaster("local[*]").setAppName("FileWordCount") 
       val ssc = new StreamingContext(conf, Seconds(30)) 
       val tableName = TableName.valueOf(name)

      var hbaseConfig = HBaseConfiguration.create()
      // broadcast a serialized config object allows us to use the same conf object among the driver and executors
      val confBroadcast = ssc.sparkContext.broadcast(new SerializableWritable(hbaseConfig))
      hbaseConfig = null
      val conn = ConnectionFactory.createConnection(confBroadcast.value.value);
   
       try {
         val admin = conn.getAdmin()
	 if (!admin.tableExists(tableName)) {
	   val tableDescriptor = new HTableDescriptor(tableName)
	   tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY))
	   admin.createTable(tableDescriptor) 
	 }
	 admin.close()
       } catch {
         case e: Exception => e.printStackTrace; throw e
       } finally {
         conn.close()
       }

       val dStream = ssc.textFileStream(inputDirectory)
       dStream.foreachRDD { rdd => 
         val wordCounts = rdd.flatMap(_.split(" ")).filter(_!="").map((_,1)).reduceByKey((a,b) => a+b)
	 wordCounts.foreachPartition {  
 	   partition => {
             val config = confBroadcast.value.value
	     val conn1 = ConnectionFactory.createConnection(config); 
             val tableName1 = TableName.valueOf(name)
             val mutator = conn1.getBufferedMutator(tableName1)	    
	     try {
	       partition.foreach{ wordCount => {
	         val (word, count) = wordCount
  	         try {
	           mutator.mutate(new Increment(Bytes.toBytes(word)).addColumn(COLUMN_FAMILY_BYTES, COLUMN_NAME_BYTES, count)) 
	         } catch {
	           // This is a possible exception we could get with BufferedMutator.mutate
	           case retries_e: RetriesExhaustedWithDetailsException => { 
		     retries_e.getCauses().foreach(_.printStackTrace)
		     println("Retries: "+retries_e.getClass)
		     throw retries_e.getCauses().get(0)
		   }
	           case e: Exception => println("General exception: "+ e.getClass); throw e
	         }
	       }   }
	     } finally {
	       mutator.close()
	       conn1.close()
	     }
	   }
	 }
       }   
       ssc.start()
       ssc.awaitTermination()
   }
}


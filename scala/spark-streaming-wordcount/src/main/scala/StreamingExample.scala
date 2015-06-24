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
    
    def main(args: Array[String]) {
       if (args.length < 3) {
	   throw new Exception("Please enter prefix name, input directory, and output table name as arguments")
       }
       val prefixName = args(0)
       val inputDirectory = args(1)
       val name = args(2)
      
       val conf = new SparkConf().setMaster("local[*]").setAppName("FileWordCount") 
       conf.set("spark.executor.extraJavaOptions", " -Xbootclasspath/p:/home/hadoop/hbase-install/lib/bigtable/alpn-boot-7.0.0.v20140317.jar")
       conf.set("spark.driver.extraJavaOptions", " -Xbootclasspath/p:/home/hadoop/hbase-install/lib/bigtable/alpn-boot-7.0.0.v20140317.jar")
       val ssc = new StreamingContext(conf, Seconds(30)) 
       val tableName = TableName.valueOf(name)
       val conn = ConnectionFactory.createConnection(); 
   
       try {
         val admin = conn.getAdmin()
	 if (!admin.tableExists(tableName)) {
	   val tableDescriptor = new HTableDescriptor(tableName)
	   tableDescriptor.addFamily(new HColumnDescriptor("WordCount"))
	   admin.createTable(tableDescriptor) 
	 }
	 admin.close()
       } catch {
         case e: Exception => e.printStackTrace; throw e
       }
       conn.close()

       val dStream = ssc.textFileStream(inputDirectory)

       def toBytes(word: String):Array[Byte] = {
         word.toCharArray.map(_.toByte)
       }

       dStream.foreachRDD { rdd => 
         rdd.foreachPartition {  partitionRecords =>
            val conn1 = ConnectionFactory.createConnection(); 
            val tableName1 = TableName.valueOf(name)
	    val mutator = conn1.getBufferedMutator(tableName1)

            partitionRecords.foreach{ line => { 
	      try {
		mutator.mutate(line.split(" ").filter(_!="").map(word => 
                new Increment(toBytes(word))
                .addColumn(toBytes("WordCount"), toBytes("Count"), 1L)).toList)
	      } catch {
	        case retries_e: RetriesExhaustedWithDetailsException => { 
	          retries_e.getCauses().foreach(_.printStackTrace); 
	  	  throw retries_e;
		}
		case e: Exception => e.printStackTrace; throw e
	      }
//  	      mutator.flush()
	      }
	    }

            try {
              mutator.close()
            } catch {
              case retries_e: RetriesExhaustedWithDetailsException => retries_e.getCauses().foreach(_.printStackTrace); throw retries_e
              case e: Exception => e.printStackTrace; throw e
            }
	    conn1.close()
	 }
       }
       ssc.start()
       ssc.awaitTermination()
   }
}


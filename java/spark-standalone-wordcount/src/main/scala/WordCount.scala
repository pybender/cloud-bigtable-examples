import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client
import java.lang.System
import java.lang.ClassNotFoundException
import java.lang.Exception
import org.apache.hadoop.mapreduce.JobContext
import scala.runtime.ScalaRunTime._
import java.util.Arrays.sort
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
import java.lang.Exception
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table       
import org.apache.hadoop.hbase.client.Increment
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.JobContext


/** word count in Spark **/

object SparkExample {
  def main(args: Array[String]) {
        if (args.length < 6) {
	   throw new Exception("Please enter prefix name, project ID, cluster name, zone name, input file path, and output table name as arguments")
	}
        val prefixName = args(0)
        val projectID = args(1)
        val clusterName = args(2)
        val zoneName = args(3)
	val file = args(4)
	val name = args(5)
	
	val masterInfo = "spark://" + prefixName + "-m:7077"
	val sc= new SparkContext(masterInfo, "WordCount") 

	// create new table if it's not already existed
        val tableName = TableName.valueOf(name)
        val confHBase = HBaseConfiguration.create()
        confHBase.set("google.bigtable.project.id", projectID);
        confHBase.set("google.bigtable.cluster.name", clusterName);
        confHBase.set("google.bigtable.zone.name", zoneName);
        confHBase.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");
        confHBase.set("spark.executor.extraJavaOptions", " -Xbootclasspath/p=/home/hadoop/alpn-boot-7.0.0.v20140317.jar")
        val conn = ConnectionFactory.createConnection(confHBase); 
   
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

        def toBytes(word: String):Array[Byte] = {
          word.toCharArray.map(_.toByte)
        }
	
	//SparkContext.textFile: reads file as a collection of lines
	val linesRDD = sc.textFile(file)
	linesRDD.foreachPartition { partitionRecords => {
	      val confHBase1 = HBaseConfiguration.create()
              confHBase1.set("google.bigtable.project.id", projectID);
	      confHBase1.set("google.bigtable.cluster.name", clusterName);
              confHBase1.set("google.bigtable.zone.name", zoneName);
	      confHBase1.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");
              val conn1 = ConnectionFactory.createConnection(confHBase1); 
              val tableName1 = TableName.valueOf(name)
              val mutator = conn1.getBufferedMutator(tableName1)

	      partitionRecords.foreach{ line => {
        	mutator.mutate(line.split(" ").filter(_!="").map{ word => 
                  new Increment(toBytes(word)).addColumn(toBytes("cf1"), toBytes("Count"), 1L)}.toList)
	      }   }
              mutator.close()
 	      conn1.close()
   	}    }   
 
	System.exit(0)

  }
}
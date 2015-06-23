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
import org.apache.hadoop.hbase.client.Put
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.JobContext
import org.apache.spark.SparkException

/** word count in Spark **/

object SparkExample {
  def main(args: Array[String]) {
        if (args.length < 7) {
	   throw new Exception("Please enter prefix name, project ID, cluster name, zone name, input file path, output table name, and expected count as arguments")
	}
        val prefixName = args(0)
        val projectID = args(1)
        val clusterName = args(2)
        val zoneName = args(3)
	val file = args(4) //file path
	val name = args(5) //output table name
	val expectedCount = args(6).toInt
	
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
	       tableDescriptor.addFamily(new HColumnDescriptor("cf"))
	       admin.createTable(tableDescriptor) 
	   }
	   admin.close()
        } catch {
          case e: Exception => e.printStackTrace; throw e
        }
        conn.close()


	val wordCounts = sc.textFile(file).flatMap(_.split(" ")).filter(_!="").map(word => (word, 1)).reduceByKey((a,b) => a+b)
        wordCounts.foreachPartition { partition => {
	    val confHBase1 = HBaseConfiguration.create()
            confHBase1.set("google.bigtable.project.id", projectID);
	    confHBase1.set("google.bigtable.cluster.name", clusterName);
            confHBase1.set("google.bigtable.zone.name", zoneName);
	    confHBase1.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");
            val conn1 = ConnectionFactory.createConnection(confHBase1); 
            val tableName1 = TableName.valueOf(name)
            val mutator = conn1.getBufferedMutator(tableName1)

            def toBytes(word: String):Array[Byte] = {
              word.toCharArray.map(_.toByte)
            }
	    
	    partition.foreach{ wordCount => {
	      val (word, count) = wordCount
  	      try {
	        mutator.mutate(new Put(toBytes(word)).addColumn(toBytes("cf"), toBytes("Count"), Bytes.toBytes(count))) 
	      } catch {
	        case retries_e: RetriesExhaustedWithDetailsException => { 
		  retries_e.getCauses().foreach(_.printStackTrace); println("retries: "+retries_e.getClass);   throw retries_e.getCauses().get(0);   	  }
	        case e: Exception => println("general exception: "+ e.getClass);   throw e
	      }
	    }   }
	}   }
	
	//validate table count
	val confValidate = HBaseConfiguration.create()
	confValidate.set("google.bigtable.project.id", projectID);
        confValidate.set("google.bigtable.cluster.name", clusterName);
        confValidate.set("google.bigtable.zone.name", zoneName);
	confValidate.set("hbase.client.connection.impl", "org.apache.hadoop.hbase.client.BigtableConnection");
        confValidate.set(TableInputFormat.INPUT_TABLE, name);
	val hBaseRDD = sc.newAPIHadoopRDD(confValidate, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result]) 	
	val count = hBaseRDD.count.toInt

	//cleanup
        val connCleanup = ConnectionFactory.createConnection(confHBase); 
        try {
          val admin = connCleanup.getAdmin()
	  admin.deleteTable(tableName)
	  admin.close()
        } catch {
          case e: Exception => e.printStackTrace; throw e
        }
        connCleanup.close()

	println("Word count = " + count)
	if (expectedCount == count) {
	  println("Word count success")
	} else {
	  println("Word count failed")
	  System.exit(1)
	}

	System.exit(0)

  }
}
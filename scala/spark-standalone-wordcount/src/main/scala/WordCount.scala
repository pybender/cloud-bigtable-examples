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
        if (args.length < 4) {
	   throw new Exception("Please enter prefix name, input file path, output table name, and expected count as arguments")
	}
        val prefixName = args(0)
	val file = args(1) //file path
	val name = args(2) //output table name
	val expectedCount = args(3).toInt
	args.foreach(println)	

	val masterInfo = "spark://" + prefixName + "-m:7077"
	val sc= new SparkContext(masterInfo, "WordCount") 

	// create new table if it's not already existed
        val tableName = TableName.valueOf(name)
        val conn = ConnectionFactory.createConnection(); 
        try {
          val admin = conn.getAdmin()
	   if (!admin.tableExists(tableName)) {
	       val tableDescriptor = new HTableDescriptor(tableName)
	       tableDescriptor.addFamily(new HColumnDescriptor("cf"))
	       admin.createTable(tableDescriptor) 
	   }
	   admin.close()
        } catch {
          case e: Exception => e.printStackTrace;  throw e
        }
        conn.close()

	val wordCounts = sc.textFile(file).flatMap(_.split(" ")).filter(_!="").map(word => (word, 1)).reduceByKey((a,b) => a+b)

        wordCounts.foreachPartition { 
 	  partition => {
	    val conn1 = ConnectionFactory.createConnection(); 
            def toBytes(word: String):Array[Byte] = {
              word.toCharArray.map(_.toByte)
            }
            val tableName1 = TableName.valueOf(name)
            val mutator = conn1.getBufferedMutator(tableName1)	    
	    partition.foreach{ wordCount => {
	      val (word, count) = wordCount
  	      try {
	        mutator.mutate(new Put(toBytes(word)).addColumn(toBytes("cf"), toBytes("Count"), Bytes.toBytes(count))) 
	      } catch {
	        case retries_e: RetriesExhaustedWithDetailsException => { 
		  retries_e.getCauses().foreach(_.printStackTrace); println("Retries: "+retries_e.getClass);   throw retries_e.getCauses().get(0); }
	        case e: Exception => println("General exception: "+ e.getClass); throw e
	      }
	    }   }
	    mutator.close()
	    conn1.close()
	  }   
	}

	//validate table count
	val confValidate = HBaseConfiguration.create()
        confValidate.set(TableInputFormat.INPUT_TABLE, name);
	val hBaseRDD = sc.newAPIHadoopRDD(confValidate, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result]) 	
	val count = hBaseRDD.count.toInt

	//cleanup
        val connCleanup = ConnectionFactory.createConnection(); 
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
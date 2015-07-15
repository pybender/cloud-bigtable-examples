package main;

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
import org.apache.hadoop.hbase.client.Put
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
import java.lang.Exception
import java.lang.Runtime._

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import connector.CloudPubsubInputDStream
import connector.CloudPubsubUtils

object CloudPubsubReceiver {
  val COLUMN_FAMILY = "WordCount"
  val COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY)
  val COLUMN_NAME_BYTES = Bytes.toBytes("Count")

  //args: pubsub_test cmd_line_1 sduskis-hello-shakespear subscription1 5
  def main(args: Array[String]) {
    if (args.length < 5) {
      throw new Exception("Please enter output table name, topic name, project name, subscription name, and sampling frequency as arguments")
    }
    val name = args(0)
    val topicName = args(1)
    val projectName = args(2)
    val subscriptionName = args(3)
    val samplingFreq = args(4)
    val sparkConf = new SparkConf().setAppName("CloudPubsubWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(samplingFreq.toInt))
    var hbaseConfig = HBaseConfiguration.create()
    // broadcast a serialized config object allows us to use the same conf object among the driver and executors
    val confBroadcast = ssc.sparkContext.broadcast(new SerializableWritable(hbaseConfig))
    hbaseConfig = null
    val conn = ConnectionFactory.createConnection(confBroadcast.value.value);
    val tableName = TableName.valueOf(name)
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

    val ackIDMessagesDStream = CloudPubsubUtils.createDirectStream(ssc, projectName, topicName, subscriptionName)
    ackIDMessagesDStream.foreach{ rdd => {
      val ackIDMessageObjects = rdd.map{ tuple3 => {
        val (ackID, messageID, messageString) = tuple3
        (ackID, (messageID, messageString))
      }   }
      // condense the list to ackID -> list of messages
      val ackIDMessageObjectsList = ackIDMessageObjects.collect().toList.groupBy(_._1).map{ case (k,v) => (k,v.map(_._2))}
      ackIDMessageObjectsList.foreach{ tuple => {
        val (ackID, messageList) = tuple
        messageList.foreach{ messageObject => {
          val (messageID, messageString) = messageObject
          val splitWords = messageString.split(" ")
          val wordsRDD = ssc.sparkContext.parallelize(splitWords)
          val wordCounts = wordsRDD.filter(_!="").map((_,1)).reduceByKey((a,b)=>a+b)
          wordCounts.foreachPartition{ partition => {
            val config = confBroadcast.value.value
            val conn1 = ConnectionFactory.createConnection(config);
            val tableName1 = TableName.valueOf(name)
            val mutator = conn1.getBufferedMutator(tableName1)
            try {
              partition.foreach{ wordCount => {
                val (word, count) = wordCount
                try {
                  mutator.mutate(new Put(Bytes.toBytes(word + "|" + messageID)).addColumn(COLUMN_FAMILY_BYTES, COLUMN_NAME_BYTES, Bytes.toBytes(count)))
                } catch {
                  // This is a possible exception we could get with BufferedMutator.mutate
                  case retries_e: RetriesExhaustedWithDetailsException => {
                    retries_e.getCauses().foreach(_.printStackTrace)
                    println("Retries: "+retries_e.getClass)
                    throw retries_e.getCauses().get(0)
                  }
                  case e: Exception => println("General exception: "+ e.getClass); throw e
                }
              }  }
            } finally {
              mutator.close()
              conn1.close()
            }
          }  }
        }  }
        val client = CloudPubsubUtils.getClient()
        CloudPubsubUtils.sendAcks(client, Array(ackID).toList, "projects/"+projectName+"/subscriptions/"+subscriptionName)
      }  }
    }   }

    ssc.start()
    ssc.awaitTermination()
  }
}

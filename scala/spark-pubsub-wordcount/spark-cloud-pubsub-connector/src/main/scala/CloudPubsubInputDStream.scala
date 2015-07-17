package connector;

import java.io.{IOException, ObjectInputStream}
import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD, ParallelCollectionRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.Logging
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.PushConfig;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import scala.collection.JavaConverters._
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.google.api.client.googleapis.json.GoogleJsonResponseException

import java.lang.reflect.InvocationTargetException

class CloudPubsubException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

class CloudPubsubInputDStream (
  @transient ssc_ : StreamingContext,
  projectName : String, 
  topicName : String,
  subscriptionName : String) extends InputDStream[(String, String, String)](ssc_) with Logging{
  private var projectFullName = "projects/" + projectName
  private var topicFullName = projectFullName + "/topics/"+ topicName
  private var subscriptionFullName = projectFullName + "/subscriptions/"+ subscriptionName
  private var BATCH_SIZE = 1000
  private var client = CloudPubsubUtils.getClient();
  private var subscriptionObject: Subscription = null

  def returnSubscriptionObject(subscriptionName: String): Subscription = {
    val existingSubscriptions = getExistingSubscriptions()
    var ret: Subscription = null
    existingSubscriptions.foreach{ thisSubscription => {
      if (thisSubscription.getName() == subscriptionFullName) {
	ret = thisSubscription
      }
    }  }
    ret
  }

  def getExistingSubscriptions(): Array[Subscription] = {
    CloudPubsubUtils.listSubscriptions(client, projectName).toArray
  }

  override def start() { 
    log.info("Starting CloudPubsubInfoDStream")
    val existingSubscription = returnSubscriptionObject(subscriptionFullName)
    if (existingSubscription != null) {
      subscriptionObject = existingSubscription
      log.info("Found an existing subscription that matches the subscription name: "+ existingSubscription)
    } else {
      log.info("Creating a new subscription")
      subscriptionObject = new Subscription().setTopic(topicFullName)
      try {
        subscriptionObject = client.projects().subscriptions()
          .create(subscriptionFullName, subscriptionObject)
          .execute();
      } catch {
        case json_response_e: GoogleJsonResponseException =>  {
          val error = json_response_e.getDetails();
          log.error(error.getMessage())
        }
        case e: Exception => log.error("General exception: "+ e.getClass); throw e
      }
    }
  }

  override def stop() { 
    //TODO this never happens even though this method is in InputDStream.scala, 
    client.projects().subscriptions().delete(subscriptionFullName).execute()
    log.info("Deleted subscription: " + subscriptionFullName)
  }

  override def compute(validTime: Time): Option[RDD[(String, String, String)]] = { //ackID, messageID, and messageContent
    val clientCompute = CloudPubsubUtils.getClient();
    val pullRequest = new PullRequest()
      .setReturnImmediately(false)
      .setMaxMessages(BATCH_SIZE);
    var messageRDD: RDD[(String, String, String)] = null
    var bool = false
    val pullResponse = clientCompute.projects().subscriptions()
      .pull(subscriptionFullName, pullRequest)
      .execute();
    val receivedMessagesAsJava = pullResponse.getReceivedMessages();
    if (receivedMessagesAsJava != null) {
      val receivedMessages = receivedMessagesAsJava.asScala
      if (receivedMessages.length != 0) {
        val idMessageSeq = receivedMessages.filter(_.getMessage()!=null).filter(_.getMessage().decodeData()!=null).map{ receivedMessage => {
          log.info("New message: " + new String(receivedMessage.getMessage().decodeData(), "UTF-8"))
          (receivedMessage.getAckId(), receivedMessage.getMessage().getMessageId(), new String(receivedMessage.getMessage().decodeData(), "UTF-8"))
        } }
        messageRDD = ssc_.sparkContext.parallelize(idMessageSeq)
	bool = true
        log.info("New message(s) at time " + validTime + ":\n" + "there are "+ messageRDD.count+ " new messages")
      }
    } else {
      log.info("Received zero new message")
    }
    if (bool) {
      Some(messageRDD)
    } else {
      None
    }
  }
}

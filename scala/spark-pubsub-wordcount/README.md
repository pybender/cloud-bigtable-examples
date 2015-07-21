Example: Integrating Spark Streaming with Cloud Pubsub

This example uses [Spark Streaming][spark-streaming] to pull for new files
every 30 seconds and perform a simple Spark job that counts the number of
times a word appears in each new file. The Spark job uses
[Cloud Bigtable][landing-page] to store the results.

[spark-streaming]: https://spark.apache.org/
[landing-page]: https://cloud.google.com/bigtable/docs/


## Contents

[[TOC]]


## Before you start

Before you run this code sample, you'll need to complete the following tasks:

1. [Create a Cloud Bigtable cluster][create-cluster]. Be sure to note the
project ID.
2. [Create a service account and a JSON key file][json-key].

[create-cluster]: creating-cluster
[json-key]: installing-hbase-client#service-account


## Creating Compute Engine VM Instances for Cloud Bigtable, Cloud Pubsub, and Spark using bdutil

Please add the following lines to extentions/bigtable/bigtable_env.sh to enable calling Cloud Pubsub API on your VMs:
[TODO]

Create new GCE VM instance with Hadoop, Spark, and Cloud Bigtable

    $ git clone https://github.com/taragu/bdutil.git
    $ cd bdutil
    $ ./bdutil -e hadoop2_env.sh -e extensions/spark/spark_env.sh -e extensions/bigtable/bigtable_env.sh -e [path/to/config/file.sh] -f deploy



## Overview of the code sample

There are three components in this example: the Spark-Cloud Pubsub connector (spark-cloud-pubsub-connector/), the message producer (cloud-pubsub-producer/), and the message processor (cloud-pubsub-receiver/).

The Spark-Cloud Pubsub connector (in the spark-cloud-pubsub-connector directory) contains two files: CloudPubsubInputDStream.scala, CloudPubsubUtils.scala, and RetryHttpInitializerWrapper.scala. CloudPubsubInputDStream.scala contains the CloudPubsubInputDStream class that extends the InputDStream class in Spark. This enables Spart streaming context object to pull messages from a Cloud Pubsub topic in the time interval that a user specifies when he/she instantiates a Spark streaming context object in their Spark application:
[INSERT CODE]

You can instantiate a CloudPubsubInputDStream object with your Cloud Pubsub information:
[INSERT CODE]

CloudPubsubUtils.scala contains utility methods that can be used either in the Spark-Cloud Pubsub connector (listSubscriptions, and getClient) or the message processor (sendAcks, and createDirectStream). 

RetryHttpInitializerWrapper.scala retries failed RPC calls, and is called in the getClient method in CloudPubsubUtils.scala

You can use the Spark-Cloud Pubsub connector in two ways. If you would like to use it without any modification, you can download a pre-built jar from a GCS storage bucket with the following command:

    	$ gsutil cp gs://cloud-bigtable-examples/spark-cloud-pubsub-connector_2.10-0.0.jar PATH/TO/SAVE/THE/FILE

If you would like modify it, you can build your modified connector with "sbt package" with the following .sbt build file:
[INSERT .SBT FILE]

In order to use the connector to build your Spark or scala application, please copy the connector jar file to the lib directory under your application (for example, cloud-pubsub-receiver/lib/spark-cloud-pubsub-connector_2.10-0.0.jar). In this examples, the connector jar is needed in both cloud-pubsub-receiver and cloud-pubsub-producer (calls utility methods in CloudPubsubUtils.scala).

The message processor (in the cloud-pubsub-receiver/ directory) is a Spark application. In the main method in CloudPubsubReceiver, the program creates a new Spark streaming context, get new messages in the form of RDDs from CloudPubsubInputDStream, and write word count of each message to Cloud Bigtable.
[INSERT CODE; almost the entire file of CloudPubsubReceiver.scala]

The message producer (in the cloud-pubsub-producer/ directory) is a scala program that reads an input file line by line, and publush each line as a message to a Cloud Pubsub topic every 1 second. 


## Building the code sample

Install SBT (a Scala compiler) on your local machine. You can skip this step
if you already have SBT on your machine. Note that these instructions are Linux
specific. Please refer to SBT’s installation page to install SBT on other
machines: http://www.scala-sbt.org/release/tutorial/Setup.html

    $ wget http://dl.bintray.com/sbt/debian/sbt-0.13.6.deb
    $ sudo dpkg -i sbt-0.13.6.deb
    $ sudo apt-get update
    $ sudo apt-get install sbt

Create a Spark project on your local machine

    $ git clone https://github.com/taragu/cloud-bigtable-examples.git
    $ cd {{github_samples_repo_name}}/scala/spark-pubsub-wordcount

We need to build three applications: the Spark-Cloud Pubsub connector, the message producer, and the message processor. To build the Spark-Cloud Pubsub connector:

   	$ cd spark-cloud-pubsub-connector
	$ sbt package

You should see the connector jar in target/scala-2.10/. Copy it
into a GCS bucket, log in to the master VM, then download the it from the GCS
bucket. You can also download the pre-built connector instead of building it on your machine with the following command:

    	$ gsutil cp gs://cloud-bigtable-examples/spark-cloud-pubsub-connector_2.10-0.0.jar PATH/TO/SAVE/THE/FILE

Next, build the message producer with the following commands:

      $ cd ../cloud-pubsub-producer
      $ cp ../spark-cloud-pubsub-connector/target/scala-2.10/spark-cloud-pubsub-connector_2.10-0.0.jar lib/
      $ sbt package

You should see the connector jar in target/scala-2.10/. Copy it
into a GCS bucket, log in to the master VM, then download the it from the GCS
bucket. You can also download the pre-built connector instead of building it on your machine with the following command:

    	$ gsutil cp gs://cloud-bigtable-examples/cloud-pubsub-producer_2.10-0.0.jar PATH/TO/SAVE/THE/FILE

Lastly, build the message processor with the following commands:

      $ cd ../cloud-pubsub-receiver
      $ cp ../spark-cloud-pubsub-connector/target/scala-2.10/spark-cloud-pubsub-connector_2.10-0.0.jar lib/
      $ sbt package

You should see the connector jar in target/scala-2.10/. Copy it
into a GCS bucket, log in to the master VM, then download the it from the GCS
bucket. You can also download the pre-built connector instead of building it on your machine with the following command:

    	$ gsutil cp gs://cloud-bigtable-examples/cloud-pubsub-receiver_2.10-0.0.jar PATH/TO/SAVE/THE/FILE





## Running the code sample with spark-submit

We need to run both the message producer and the message processor at the same time: the message producer publishes messages, and the message processor transforms the data and writes them to Cloud Bigtable. Run the following commands to run your application with spark-submit.

Log in to the master as user hadoop:

    $ gcloud --project=[PROJECT_ID] compute ssh --zone=[ZONE] hadoop@[PREFIX]-m
    $ cd spark-install

Download a text file to the /home/hadoop directory:

    $ curl -f http://www.gutenberg.org/cache/epub/1112/pg1112.txt > romeo_juliet.txt

You can download the message producer and message processor jars you submit in the previous section
“Building the code sample”, or download a pre-compiled jar with the following
command:

    $ gsutil cp gs://cloud-bigtable-examples/cloud-pubsub-receiver_2.10-0.0.jar .
    $ gsutil cp gs://cloud-bigtable-examples/cloud-pubsub-producer_2.10-0.0.jar .

We also need to Cloud Pubsub API jar as well as the Spark-Cloud Pubsub connector on Spark's classpath in order to call their API in runtime. Download the Cloud Pubsub API jar and the connector jar with the following commands:

   $ wget http://central.maven.org/maven2/com/google/apis/google-api-services-pubsub/v1-rev2-1.20.0/google-api-services-pubsub-v1-rev2-1.20.0.jar
   $ gsutil cp gs://cloud-bigtable-examples/spark-cloud-pubsub-connector_2.10-0.0.jar .

Next, create a new topic in the Cloud Pubsub web UI. Please note the topic name.

Open two terminals (A and B). In terminal A, we run the message producer; in terminal B, we run the message processor. 

In terminal B, run the message processor with the following command:

       $ bigtable-spark-submit --extraJars /home/hadoop/google-api-services-pubsub-v1-rev2-1.20.0.jar,/home/hadoop/spark-cloud-pubsub-connector_2.10-0.0.jar cloud-pubsub-receiver_2.10-0.0.jar pubsub_test [TOPIC_NAME] [PROJECT_ID] subscription1 5 

In terminal A, run the message producer with the following command:

   $ sbt "project cloud-pubsub-producer" "run [PROJECT_ID] [TOPIC_NAME] romeo_juliet.txt"

{% endblock %}

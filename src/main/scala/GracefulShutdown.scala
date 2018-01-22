package com.isecc

import sun.misc.Signal
import sun.misc.SignalHandler

import java.nio.file.{FileSystems, Files}
import java.util.Date

import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//This is a system tool for investigating the interatcions between a SparkStreaming Job and YARN
//during controlled/graceful shutdown
//developer: Evo Eftimov

object GracefulShutdown {

  var configFilePath = "/tmp/test-config.conf"

  def main(args: Array[String]) {

    var shutdownFlag: String = "file:///tmp/shutdownmarker"
    var bootstrapBrokers: String = "localhost:9092"
    var topicName: String = "test_topic"
    var groupName: String = "group-1"
    var resetOffset: String = "earliest"
    var autoCommit: String = "false"
    var master: String = "local[3]"
    var deploymentMode: String = "client"
    var batchInterval: Int = 10
    var doCommit: Boolean = false
    var secProtocol:String = "SASL_PLAINTEXT" //PLAINTEXT
    var zookeeper = "localhost:2081"


    if (args.length == 1) {

      configFilePath = args(0)
      val source = Source.fromFile(configFilePath)
      val lines = source.getLines()

      println("the config params used:")

      shutdownFlag = lines.next()
      println(shutdownFlag)
      bootstrapBrokers = lines.next()
      println(bootstrapBrokers)
      topicName = lines.next()
      println(topicName)
      groupName = lines.next()
      println(groupName)
      resetOffset = lines.next()
      println(resetOffset)
      autoCommit = lines.next()
      println(autoCommit)
      master = lines.next()
      println(master)
      deploymentMode = lines.next()
      println(deploymentMode)
      batchInterval = lines.next().toInt
      println(batchInterval)
      doCommit = lines.next().toBoolean
      println(doCommit)
      secProtocol = lines.next()
      println(secProtocol)
      zookeeper = lines.next()
      println(zookeeper)

    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupName,
      "auto.offset.reset" -> resetOffset,
      "enable.auto.commit" -> autoCommit,
      "security.protocol" ->  secProtocol,
      "zookeeper.connect" -> zookeeper
    )

    //replaces the signal handler in the Spark Framework - this allow to measure the time
    //between the SIGTERM and SIGKILL signals sent by YARN during "yarn -kill"
    Signal.handle(new Signal("INT"), new SignalHandler() {

      var stopFlag = false
      val fs = FileSystem.get(new Configuration())

      def handle(sig: Signal) {

        println("*********** SIGTERM received at: " + new Date() + "**************")

        while (!stopFlag) {
          println("SIGTERM Signal Handler iteration: " + new Date())
          Thread.sleep(2000)
          stopFlag = fs.exists(new Path(shutdownFlag))
        }

      }
    })



    val conf = new SparkConf().setAppName("SparkStreamingGracefulShutdown-Tester").set("spark.streaming.stopGracefullyOnShutdown", "true")

    if(master.contains("yarn")){

      if(deploymentMode.contains("client")) {
        conf.set("spark.master", master);
        conf.set("spark.submit.deployMode", deploymentMode);
      }else{
        conf.set("spark.master", master);
        conf.set("spark.submit.deployMode", deploymentMode);
      }

    }else if (master.contains("local")){

      conf.set("spark.master", master);

    }

    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    ssc.sparkContext.setLogLevel("DEBUG")

    val topics = Array(topicName)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val message = stream.map(record => (record.key, record.value))

    message.print()

    stream.foreachRDD { rdd =>

      if(doCommit) {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    }

    ssc.start()
    val contextManagerThread = new ContextManager(shutdownFlag, ssc, shutdownFlag)
    contextManagerThread.run()
    ssc.awaitTermination()
    //thread.join()

  }

}


class ContextManager(filePath: String, ssc: StreamingContext, shutdownFlag: String) extends Thread {

  override def run(): Unit = {

    var stopFlag = false

    val fs = FileSystem.get(new Configuration())

    while (!stopFlag) {
      println("Spark Context Management Thread iteration: " + new Date())
      Thread.sleep(2000)
      stopFlag = fs.exists(new Path(shutdownFlag))
    }

    ssc.stop(true, true)

  }


}


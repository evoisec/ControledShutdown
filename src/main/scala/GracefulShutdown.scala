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
　
    var session_timeout = "30000"
　
    var max_poll_interval = "30000"
    var heartbeat_interval = "15000"
　
　
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
　
      session_timeout = lines.next()
      println(session_timeout)
　
      max_poll_interval = lines.next()
      println(max_poll_interval)
      heartbeat_interval = lines.next()
      println(heartbeat_interval)
　
　
    }
　
    //https://stackoverflow.com/questions/39730126/difference-between-session-timeout-ms-and-max-poll-interval-ms-for-kafka-0-10-0
    //kafka
    //max.poll.interval.ms for 0.10.0
    //or for 0.10.1
    //session.timeout.ms=30000
    //max.poll.interval.ms (processing)
　
    //for client param session.timeout.ms - Note that the value must be in the allowable range as configured in the broker configuration by
    //group.min.session.timeout.ms and group.max.session.timeout.ms
　
    //sent from group leader to consumer threads
    //http://grokbase.com/t/kafka/users/1628wmqfqd/session-timeout-and-heartbeat-interval
    //heartbeat.interval.ms
　
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupName,
      "auto.offset.reset" -> resetOffset,
      "enable.auto.commit" -> autoCommit,
      "security.protocol" ->  secProtocol,
      "zookeeper.connect" -> zookeeper,
　
      "session.timeout.ms" -> session_timeout, //old param
　
      "max.poll.interval.ms" -> max_poll_interval,
      "heartbeat.interval.ms" -> heartbeat_interval
    )
　
    Signal.handle(new Signal("TERM"), new SignalHandler() {
　
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
　
    conf.set("spark.streaming.backpressure.enabled", "true")
　
    //spark.streaming.receiver.maxRate
　
　
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
          println("==== offset log start =====")
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          println("==== offset log end =====")
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
　
　

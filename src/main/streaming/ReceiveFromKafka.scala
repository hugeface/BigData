package main.streaming

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

object ReceiveFromKafka {

  case class Order(order_id:String, user_id:String)

  def main(args: Array[String]): Unit = {
    // 0.对输出日志做控制
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // 1.获取参数/指定6个参数
    val Array(group_id, topic, exectime, dt) = Array("group_test", "stream", "30", "20181206")
    val zkHostIP = Array("10", "11", "12").map("192.168.136" + _)
    val ZK_QUORUM = zkHostIP.map(_+":2181").mkString(",")
    val numPartitions = 1

    // 2.创建streamContext
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))

    // topic 对应线程Map{topic：numThreads}
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_,numPartitions.toInt)).toMap

    // 3.通过Receiver接受kafka数据
    // createStream return DStream of (Kafka message key, Kafka message value)
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    // 4.生成一个RDD转DF的方法
    def rdd2DF(rdd: RDD[String]):DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming From Kafka")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport().getOrCreate()
      import spark.implicits._

      rdd.map(x => {
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id, mess.user_id)
      }).toDF()
    }

    // 5.DStream核心处理逻辑，对DStream中每个RDD做"RDD转DF"，然后通过DF结构将数据最佳到分区表中
    val log = mesR.foreachRDD(rdd => {
      val df = rdd2DF(rdd)
      df.withColumn("dt", lit(dt.toString))
        .write.mode(SaveMode.Append)
        .insertInto("streaming.order_partition")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

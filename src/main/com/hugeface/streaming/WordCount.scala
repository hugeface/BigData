package main.com.hugeface.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    // Using updateStateByKey requires the checkpoint directory to be configured
    ssc.checkpoint("/Users/arete/Source/DATA")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (curValues:Seq[Long], preValueState:Option[Long]) => {
      val curCount = curValues.sum
      val preCount = preValueState.getOrElse(0L)
      Some(curCount + preCount)
    }

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map((_, 1L)).updateStateByKey[Long](addFunc)
    wordCount.print()

    // Start receiving data and processing it
    ssc.start()
    // Wait for the  processing stopped
    ssc.awaitTermination()
  }
}

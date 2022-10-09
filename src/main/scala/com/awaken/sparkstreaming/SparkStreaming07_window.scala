package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/9 21:17
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val word2OneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val wordToOneByWindow: DStream[(String, Int)] = word2OneDStream.window(Seconds(12), Seconds(6))

    val wordToCountDStream: DStream[(String, Int)] = wordToOneByWindow.reduceByKey(_+_)
    wordToCountDStream.print()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

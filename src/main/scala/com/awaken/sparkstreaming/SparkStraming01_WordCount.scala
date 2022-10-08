package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/8 22:59
 */
object SparkStraming01_WordCount {
  def main(args: Array[String]): Unit = {
    // 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    // StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordToOneDStream: DStream[(String, Int)] = wordStream.map((_,1))

    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

    wordToSumDStream.print()

    // 启动
    ssc.start()
    // 将主线程阻塞，主线程不退出
    ssc.awaitTermination()

  }
}

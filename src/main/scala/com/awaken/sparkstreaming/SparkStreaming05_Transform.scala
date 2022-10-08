package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/9 0:31
 */
object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    // 在 Driver 端执行，全局一次
    println("111111111111" + Thread.currentThread().getName)

    // 转换为 RDD 操作
    val wordToSumDStream: DStream[(String, Int)] = lineDStream.transform(
      rdd => {
        // 在 Driver 端执行(ctrl + n JobGenerator) , 一个批次执行一次
        println("22222222222" + Thread.currentThread().getName)

        val words: RDD[String] = rdd.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map(x => {
          // 在 Executor端执行, 和单词个数相同
          println("33333333333333" + Thread.currentThread().getName)
          (x, 1)
        })
        val result: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        result
      }
    )
    wordToSumDStream.print()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/9 21:43
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineDStream = ssc.socketTextStream("hadoop102", 9999)

    val wordToOneDStream = lineDStream.flatMap(_.split(" ")).map((_, 1))

    wordToOneDStream.foreachRDD(
      rdd => {
        // 在 Driver 端执行 (JobScheduler), 一个批次一次
        // 在 JobScheduler中查找 streaming-job-executor
        println("222222:" + Thread.currentThread().getName)

        //创建mysql连接对象  //driver

        rdd.foreachPartition(
          //创建mysql连接对象  //executor
          //5.1 测试代码
          iter=>iter.foreach(println)
          //5.2 业务代码
          //5.2.1 获取连接
          //5.2.2 操作数据，使用连接写库
          //5.2.3 关闭连接
        )
      }
    )
    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

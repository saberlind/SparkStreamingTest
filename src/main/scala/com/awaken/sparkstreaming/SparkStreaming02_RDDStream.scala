package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Awaken
 * @create 2022/10/8 23:21
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(4))

    // 创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // 创建 QueueInputDStream
    // oneAtATime = true 默认，一次读取队列里的一个数据
    // oneAtATime = false， 按照设定的批次时间，读取队列里面数据
    val inputDStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    // 处理队列中的数据
    val sumDStream: DStream[Int] = inputDStream.reduce(_+_)

    sumDStream.print()

    ssc.start()

    for (elem <- -1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination();

  }

}

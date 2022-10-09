package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_reduceByKeyAndWindow {

    def main(args: Array[String]): Unit = {

        // 1 初始化SparkStreamingContext
        val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(3))

        // 保存数据到检查点
        ssc.checkpoint("./ck")

        // 2 通过监控端口创建DStream，读进来的数据为一行行
        val lines = ssc.socketTextStream("hadoop102", 9988)

        // 3 切割=》变换
        val wordToOne = lines.flatMap(_.split(" ")).map((_, 1))

        // 4 窗口参数说明： 算法逻辑，窗口12秒，滑步6秒
        val wordCounts = wordToOne.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))

        // 5 打印
        wordCounts.print()

        // 6 启动=》阻塞
        ssc.start()
        ssc.awaitTermination()
    }
}
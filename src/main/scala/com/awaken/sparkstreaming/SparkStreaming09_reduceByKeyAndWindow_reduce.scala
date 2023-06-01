package com.awaken.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_reduceByKeyAndWindow_reduce {

    def main(args: Array[String]): Unit = {

        // 1 初始化SparkStreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
        val ssc = new StreamingContext(conf, Seconds(3))

        // 保存数据到检查点
        ssc.checkpoint("./ck")

        // 2 通过监控端口创建DStream，读进来的数据为一行行
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

        // 3 切割 =》变换
        val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

        // 4 窗口参数说明： 算法逻辑，窗口12秒，滑步6秒
//        val wordToSumDStream: DStream[(String, Int)]= wordToOne.reduceByKeyAndWindow(
//            (a: Int, b: Int) => (a + b),
//            (x: Int, y: Int) => (x - y),
//            Seconds(12),
//            Seconds(6)
//        )

        // 处理单词统计次数为0的问题
        val wordToSumDStream: DStream[(String, Int)]= wordToOne.reduceByKeyAndWindow(
            (a: Int, b: Int) => (a + b),
            (x: Int, y: Int) => (x + y),
            Seconds(12),
            Seconds(6),
            new HashPartitioner(2),
            (x:(String, Int)) => x._2 > 0
        )

        // 5 打印
        wordToSumDStream.print()

        // 6 启动=》阻塞
        ssc.start()
        ssc.awaitTermination()
    }
}
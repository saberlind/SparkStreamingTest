package com.awaken.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/9 20:43
 */
object SparkStreaming06_updateStateByKey {
   def main(args: Array[String]): Unit = {
     // 创建SparkConf
     val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

     // 创建StreamingContext
     val ssc = new StreamingContext(sparkConf, Seconds(3))

     // 使用 updateStateByKey 必须要设置检查点目录
     ssc.checkpoint("E:\\IDEAworkspace\\SparkStreamingTest\\checkpoint")

     // 获取一行数据
     val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

     val wordDtream: DStream[String] = lineDStream.flatMap(_.split(" "))

     val word2OneDtream: DStream[(String, Int)] = wordDtream.map((_,1))

     // 使用 updateStateByKey 来更新状态，统计从运行开始以来单词总的次数
     val result: DStream[(String, Int)] = word2OneDtream.updateStateByKey(updateFunc)

     result.print()

     // 开启任务
     ssc.start()
     ssc.awaitTermination()
   }
  def updateFunc = (seq: Seq[Int], state: Option[Int]) => {
    // 获取当前批次单词的和
    val currentCount: Int = seq.sum
    // 获取历史状态的数据
    val stateCount: Int = state.getOrElse(0)
    // 将当前批次的和加上历史状态的数据和，返回
    Some(currentCount + stateCount)
  }
}

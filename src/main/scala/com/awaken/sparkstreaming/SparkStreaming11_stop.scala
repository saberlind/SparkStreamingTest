package com.awaken.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author Awaken
 * @create 2022/10/9 22:01
 */
object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    // 设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 接收数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    // 执行业务逻辑
    lineDStream.flatMap(_.split(" ")).map((_,1)).print()

    // 开启监控程序
    new Thread(new MonitorStop(ssc)).start()

    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

class MonitorStop(context: StreamingContext) extends Runnable {
  override def run(): Unit = {
    // 获取 HDFS 文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "saberlind")

    while (true) {
      Thread.sleep(5000)
      // 获取/stopSpark路径是否存在
      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))

      if (result) {
        val state: StreamingContextState = context.getState()
        // 获取当前任务是否正在执行
        if (state == StreamingContextState.ACTIVE) {
          // 优雅关闭
          context.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }

    }
  }
}

package com.awaken.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Awaken
 * @create 2022/10/8 23:37
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val result: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    result.print()

    ssc.start()

    ssc.awaitTermination()


  }
}

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // receiver 刚启动的时候，调用该方法，作用为：读数据并将数据发送给 Spark
  override def onStart(): Unit = {
    // 在 onStart 方法里面创建一个线程，专门用来接收数据
    new Thread() {
      override def run() {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {
  }

  // 读数据并将数据发送给 Spark
  def receive(): Unit = {

    // 创建一个 socket
    val socket: Socket = new Socket(host, port)

    // 字节流读取数据不方便，转换成字符流 buffer，方便整行读取
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    // 读取数据
    var input: String = reader.readLine()

    // 当receiver 没有关闭并且输入数据不为空，就循环发送数据给 Spark
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    // 如果循环解释，则关闭资源
    reader.close()
    socket.close()

  }

}

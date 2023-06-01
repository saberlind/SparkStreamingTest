package com.awaken.sparkstreaming

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.awaken.sparkstreamimg.DriverAgent
import com.awaken.sparkstreaming.entity.MyDriver
import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
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

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))

    // 最佳实践：在此处创建一个 Driver的代理对象，全局只执行一次
//    val agent: Driver = new DriverAgent()
    val agent = new MyDriver()
    println("创建 driver-agent => " +agent.toString)

    wordToOneDStream.foreachRDD(
      rdd => {
        // 在 Driver 端执行 (JobScheduler), 一个批次一次但是会有序列化问题
        // 在 JobScheduler中查找 streaming-job-executor
        println("222222:" + Thread.currentThread().getName)

        //创建mysql连接对象  //driver (此处创建会有序列化问题)

        rdd.foreachPartition {
          //5.1 测试代码
          iter => {
            //创建mysql连接对象  //executor 在此处创建最佳? (不使用代理的情况下最佳)
            //            val driver = new Driver()
            //5.2 业务代码
            while (iter.hasNext) {
              val s: String = iter.next()._1
              var sql: String = "insert into sensor values ('" + s + "', 1607527992000, 70)"
              println(sql)
              save.saveByConnection(agent, sql)
            }
          }
        }
      }
    )
    // 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

object save {
  def saveByConnection(driver: Driver, sql: String): Unit = {
//    val driver = new Driver() 不要在此处创建，效率低下
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    //5.2.1 获取连接
    val connection: Connection = driver.connect("jdbc:mysql://hadoop102:3306/test", properties)
    connection.setAutoCommit(false)
    val statement: PreparedStatement = connection.prepareStatement(sql)
    statement.execute()
    connection.commit()
    //5.2.2 操作数据，使用连接写库
    //5.2.3 关闭连接
    connection.close()
  }
}

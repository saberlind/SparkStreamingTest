package com.awaken.sparkstreaming.entity

import java.util.logging.Logger

import com.mysql.jdbc.Driver

/**
 * @author Awaken
 * @create 2023/6/1 22:07
 * @Topic
 */
class MyDriver extends Driver with Serializable {

  override def getParentLogger: Logger = null

}

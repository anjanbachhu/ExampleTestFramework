package org.anjan.Utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkCommon {

  lazy val conf = {
    new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("AnjanTestFrameWork")
  }

  lazy val sparkContext = new SparkContext(conf)
  lazy val sparkSQLContext = SQLContext.getOrCreate(sparkContext)
  lazy val streamingContext = StreamingContext.getActive()
    .getOrElse(new StreamingContext(sparkContext, Seconds(2)))

}

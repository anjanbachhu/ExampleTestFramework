package org.anjan

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkContextTestSpec extends BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }
  def sc: SparkContext = _sc
}

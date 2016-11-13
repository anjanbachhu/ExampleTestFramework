package org.anjan

import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSqlTestSpec extends SparkContextTestSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  def sqlc: SQLContext = _sqlc
}

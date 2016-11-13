package org.anjan

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object EmployeeMain_SkSql {

  def main(args: Array[String]): Unit = {

    // To disable/enable the Logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = Utils.SparkCommon.sparkContext
    val sqlc = Utils.SparkCommon.sparkSQLContext

    val employeeDao = new EmployeeDao(sqlc)
    import sqlc.implicits._

    //To read data from text file
    val employees = sc.textFile("src/main/resources/data/employees.txt")
      .map(_.split(","))
      .map(fields => Employee(fields(0), fields(1), fields(2), fields(3).trim.toInt))
      .toDF()
    employees.registerTempTable("employees")


    //To read from parquet file
    /*
    val employeesParquetdf = sqlc.read.parquet("src/main/resources/data/employeesParquet")
    employeesParquetdf.registerTempTable("employees")
    */

    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info("Select the last name of all employees")
    employeeDao.lastNames().collect().foreach(log.info(_))

    //To save as parquet file
    //employees.write.parquet("src/main/resources/data/employeesParquet")
  }
}

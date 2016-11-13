package org.anjan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


class EmployeeDao (sqlc: SQLContext) {

  def lastNames(): RDD[String] =
    sqlc
      .sql("SELECT lastName FROM employees")
      .map(row => row.getString(0))


  //optional
  def distinctLastNames(): RDD[String] =
    sqlc
      .sql("SELECT DISTINCT lastName FROM employees")
      .map(row => row.getString(0))

  def byLastName(lastNames: String*): RDD[Employee] =
    sqlc
      .sql(s"SELECT * FROM employees WHERE lastName IN(${lastNames.mkString("'", "', '", "'")})")
      .map(EmployeeDao.toEmployee)

  def byLastNameLike(lastName: String): RDD[Employee] =
    sqlc
      .sql(s"SELECT * FROM employees WHERE lastName LIKE '$lastName%'")
      .map(EmployeeDao.toEmployee)
}

object EmployeeDao {
  private def toEmployee(row: Row): Employee =
    Employee(row.getString(0), row.getString(1), row.getString(2), row.getInt(3))
}

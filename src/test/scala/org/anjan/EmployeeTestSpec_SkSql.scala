package org.anjan

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class EmployeeTestSpec_SkSql extends FlatSpec with SparkSqlTestSpec with GivenWhenThen with Matchers {

  behavior of "Employee Records"

  val employees = Array(
    Employee("123234877", "Michael", "Rogers", 14),
    Employee("152934485", "Anand", "Manikutty", 14),
    Employee("222364883", "Carol", "Smith", 37),
    Employee("326587417", "Joe", "Stevens", 37),
    Employee("332154719", "Mary-Anne", "Foster", 14),
    Employee("332569843", "George", "ODonnell", 77),
    Employee("546523478", "John", "Doe", 59),
    Employee("631231482", "David", "Smith", 77),
    Employee("654873219", "Zacary", "Efron", 59),
    Employee("745685214", "Eric", "Goldsmith", 59),
    Employee("845657245", "Elizabeth", "Doe", 14),
    Employee("845657246", "Kumar", "Swamy", 14)
  )
  val departments = Array(
    Department(14, "IT", 65000),
    Department(37, "Accounting", 15000),
    Department(59, "Human Resources", 240000),
    Department(77, "Research", 55000)
  )
  var employeeDao: EmployeeDao = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val _sqlc = sqlc
    import _sqlc.implicits._
    sc.parallelize(employees).toDF().registerTempTable("employees")
    sc.parallelize(departments).toDF().registerTempTable("departments")
    employeeDao = new EmployeeDao(sqlc)
  }

  "The last name of all employees" should "be selected" in {
    val lastNames = employeeDao.lastNames().collect()
    lastNames should have length 12
  }

  //optionl
  "The last name of all employees" should "be selected without duplicates" in {
    val distinctLastNames = employeeDao.distinctLastNames().collect()
    distinctLastNames should have length 10
  }

  "The employees whose last name is 'Smith'" should "be selected" in {
    val smiths = employeeDao.byLastName("Smith").collect()
    smiths should equal(Array(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("631231482", "David", "Smith", 77)
    ))
  }

  "The employees whose last name is 'Smith' or 'Doe'" should "be selected" in {
    val smithsOrDoes = employeeDao.byLastName("Smith", "Doe").collect()

    smithsOrDoes should equal(Array(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("546523478", "John", "Doe", 59),
      Employee("631231482", "David", "Smith", 77),
      Employee("845657245", "Elizabeth", "Doe", 14)
    ))
  }

  "The employees whose last name name begins with an 'S'" should "be selected" in {
    val smithsOrDoes = employeeDao.byLastNameLike("S").collect()

    smithsOrDoes should equal(Array(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("326587417", "Joe", "Stevens", 37),
      Employee("631231482", "David", "Smith", 77),
      Employee("845657246", "Kumar", "Swamy", 14)
    ))
  }
}

package prv.saevel.spark.sql.workshop.dataframes

import org.apache.spark.sql.{DataFrame, SQLContext}

object EmployeeSalaryLens {

  def apply(department: String, minSalary: Double, maxSalary: Double)
           (employees: DataFrame, assignments: DataFrame, salaries: DataFrame)
           (implicit sqlContext: SQLContext): DataFrame = ???
}

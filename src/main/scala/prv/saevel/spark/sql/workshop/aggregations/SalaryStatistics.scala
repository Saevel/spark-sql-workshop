package prv.saevel.spark.sql.workshop.aggregations

import org.apache.spark.sql.{DataFrame, SQLContext}

object SalaryStatistics {

  def apply(employees: DataFrame, assignments: DataFrame, salaries: DataFrame)
           (implicit sqlContext: SQLContext): DataFrame = ???
}

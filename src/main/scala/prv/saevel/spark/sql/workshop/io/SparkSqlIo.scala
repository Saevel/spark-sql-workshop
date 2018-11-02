package prv.saevel.spark.sql.workshop.io

import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkSqlIo {

  def readJson(file: String)(implicit sqlContext: SQLContext): DataFrame = ???

  def readCsv(file: String)(implicit sqlContext: SQLContext): DataFrame = ???

  def readOrc(file: String)(implicit sqlContext: SQLContext): DataFrame = ???
}
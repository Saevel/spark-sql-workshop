package prv.saevel.spark.sql.workshop.io

import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkSqlIo {

  def readJson(file: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.json(file)
  }

  def readCsv(file: String)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext
      .textFile(file)
      .map(_.split(","))
      .map(s => Survey(s(0).toLong, s(1), s(2), s(3).toInt)).toDF
  }

  def readOrc(file: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.orc(file)
  }
}
package prv.saevel.spark.sql.workshop

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

trait SparkContextProvider {

  protected def withSparkContext[T](master: String, appName: String, optionalMetastoreDir: Option[String] = None)
                                   (f: SparkContext => T): T = {

    val basicConf = new SparkConf().setAppName(appName).setMaster(master)
    val conf = optionalMetastoreDir.fold[SparkConf](basicConf)(warehouseDir => basicConf.set("spark.sql.warehouse.dir", warehouseDir))

    val contextAttempt = Try(new SparkContext(conf))
    val resultAttempt = contextAttempt.map(f)

    contextAttempt.foreach(_.stop)

    resultAttempt match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }

  protected def withHiveContext[T](f: HiveContext => T)(implicit sparkContext: SparkContext): T = {
    Try(new HiveContext(sparkContext)).map(f) match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }
}

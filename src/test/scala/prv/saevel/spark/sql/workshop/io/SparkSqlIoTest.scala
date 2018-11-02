package prv.saevel.spark.sql.workshop.io

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import prv.saevel.spark.sql.workshop.{BasicGenerators, FileUtils, SparkContextProvider, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class SparkSqlIoTest extends WordSpec with Matchers with PropertyChecks with BasicGenerators with SparkContextProvider
  with StaticPropertyChecks with FileUtils with BeforeAndAfterAll {

  private implicit val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("SparkSqlIoTest"))

  private implicit val sqlContext = new HiveContext(sparkContext)

  private val surveys: Gen[List[Survey]] = Gen.choose(1, 100).flatMap( n => Gen.listOfN(n, for {
    id <- Gen.choose(0, 1000)
    shortDescLength <- Gen.choose(1, 20)
    shortDescription <- stringOfLength(shortDescLength)
    longDescLength <- Gen.choose(5, 100)
    longDescription <- stringOfLength(longDescLength)
    rating <- Gen.choose(1, 5)
  } yield Survey(id, shortDescription, longDescription, rating)))

  private val jsonFilePath = "target/json"

  private val csvFilePath = "target/csv"

  private val orcFilePath = "target/orc"

  private val metastoreDirectory = "metastore_db"

  override def beforeAll: Unit = {
    Files.createDirectories(Paths.get(System.getProperty("user.dir")).resolve("target"))
  }

  "readJson" should {

    "read arbitrary data from a given JSON file" in withDirectoryRemoved(jsonFilePath){
      forOneOf(surveys){ surveys =>
        import sqlContext.implicits._
        surveys.toDF.write.json(jsonFilePath)

        val surveysRetrieved = SparkSqlIo.readJson(jsonFilePath).as[Survey].collect
        surveysRetrieved should contain theSameElementsAs(surveys)
      }
    }
  }

  "readCsv" should {

    "read arbitrary data from a given CSV file" in withDirectoryRemoved(csvFilePath){
      forOneOf(surveys){ surveys =>

        import sqlContext.implicits._
        sparkContext
          .parallelize(surveys)
          .map(survey => s"${survey.id},${survey.shortDescription},${survey.longDescription},${survey.rating}")
          .saveAsTextFile(csvFilePath)

        val surveysRetrieved = SparkSqlIo.readCsv(csvFilePath).as[Survey].collect

        surveysRetrieved should contain theSameElementsAs(surveys)
      }
    }
  }

  "readOrc" should {

    "read arbitrary data from a given ORC file" in withDirectoryRemoved(orcFilePath){
      forOneOf(surveys){ surveys =>

        import sqlContext.implicits._
        surveys.toDF.write.orc(orcFilePath)

        val surveysRetrieved = SparkSqlIo.readOrc(orcFilePath).as[Survey].collect

        surveysRetrieved should contain theSameElementsAs(surveys)
      }
    }
  }
}

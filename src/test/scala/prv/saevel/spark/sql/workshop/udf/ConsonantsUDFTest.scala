package prv.saevel.spark.sql.workshop.udf

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner
import prv.saevel.spark.sql.workshop.{BasicGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class ConsonantsUDFTest extends WordSpec with Matchers with StaticPropertyChecks with BasicGenerators {

  private val shortStrings = Gen.choose(5, 10).flatMap(stringOfLength)

  private implicit val arrayEncoder = Encoders.javaSerialization[Array[String]]

  private val line: Gen[String] = for {
    n <- Gen.choose(3, 15)
    words <- Gen.listOfN(n, shortStrings)
  } yield words.mkString(" ")

  private val lines: Gen[List[String]] = for {
    n <- Gen.choose(3, 10)
    lines <- Gen.listOfN(n, line)
  } yield lines

  private implicit val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("TokenizerUDFTest"))

  private implicit val sqlContext = new HiveContext(sparkContext)

  "ConsonantsUDF" should {

    "register a UDF that removes vowels from Strings" in forOneOf(lines){ lines =>
      import sqlContext.implicits._

      val consonants = ConsonantsUDF()

      val linesDF = sparkContext.parallelize(lines).toDF("text")

      val results = linesDF.select(consonants($"text")).as[String].collect

      val expectedResults = lines.map(toConsonants)

      results should contain theSameElementsAs(expectedResults)
    }
  }

  private val vowels: Array[Char] = Array('a', 'i', 'u', 'e', 'o')

  private def toConsonants(s: String): String = s.filterNot(c => vowels.contains(c.toLower))
}

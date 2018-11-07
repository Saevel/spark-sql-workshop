package prv.saevel.spark.sql.workshop.udaf

import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner
import prv.saevel.spark.sql.workshop.{BasicGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class HarmonicMeanUDAFTest extends WordSpec with Matchers with StaticPropertyChecks with BasicGenerators {

  private val positiveNumbers = Gen.choose(Double.MinPositiveValue, Double.MaxValue)

  private val positiveNumberLists = Gen.choose(5, 15).flatMap(Gen.listOfN(_, positiveNumbers))

  private implicit val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("HarmonicMeanUDAFTest"))

  private implicit val sqlContext = new HiveContext(sparkContext)

  "HarmonicMeanUDAF" should {

    "calculate the harmonic mean for the same numeric field in multiple rows" in forOneOf(positiveNumberLists){ numbers =>

      import sqlContext.implicits._

      val numbersDF = numbers.toDS.select($"value".as[Double]).toDF

      val expectedValue = (numbers.size / (numbers.foldLeft(0.0)((accumulator, x) => accumulator + (1 / x))))

      val actualValue = numbersDF
        .select(HarmonicMeanUDAF($"value") as "harmonic_mean")
        .select($"harmonic_mean")
        .as[Double]
        .first

      actualValue should equal(expectedValue)
    }
  }
}

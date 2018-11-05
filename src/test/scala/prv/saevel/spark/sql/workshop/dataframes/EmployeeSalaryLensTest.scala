package prv.saevel.spark.sql.workshop.dataframes

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import prv.saevel.spark.sql.workshop.{BasicGenerators, StaticPropertyChecks}

@RunWith(classOf[JUnitRunner])
class EmployeeSalaryLensTest extends WordSpec with PropertyChecks with Matchers with BasicGenerators with StaticPropertyChecks {

  private implicit val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("EmplyeeSalaryLensTest"))

  private implicit val sqlContext = new HiveContext(sparkContext)

  private implicit def tupleEncoder(implicit e1: Encoder[String], e2: Encoder[Long]): Encoder[(String, String, Long)] =
    Encoders.tuple[String, String, Long](e1, e1, e2)

  private val ids: Gen[Long] = Gen.choose(1, 100000)

  private val departments: Gen[String] = Gen.oneOf("HR", "Accounting", "Development", "DBA", "Communications", "Corporate Services")

  private def incomeBrackets(min: Double, max:Double): Gen[(Double, Double)] = for {
    lowerBracket <- Gen.choose(min, max)
    upperBrakcet <- Gen.choose(lowerBracket, max)
  } yield (lowerBracket, upperBrakcet)

  def employeeDataByDepartmentAndIncomeBracket(size: Int,
                                               department: String,
                                               minIncome: Double,
                                               maxIncome: Double): Gen[List[(Employee, Assignment, Salary)]] =
    Gen.listOfN(size, for {
      id <- ids
      nameLength <- Gen.choose(5, 10)
      name <- stringOfLength(nameLength)
      surnameLength <- Gen.choose(5, 10)
      surname <- stringOfLength(surnameLength)
      age <- Gen.choose(20, 100)
      salary <- Gen.choose(minIncome, maxIncome)
    } yield (Employee(id, name, surname, age), Assignment(id, department), Salary(id, salary)))

  private def brackets(n: Int, min: Double, max: Double): Gen[List[(Double, Double)]] =
    Gen.listOfN(n, Gen.choose(min, max)).flatMap(list =>
      list.sorted.grouped(2).filter(_.size == 2).toList.map { case List(lower, upper) => (lower, upper)}
    )

  private def employees(department: String, employeesPerBracketGenerator: Gen[Int],
                        bracketsGen: Gen[List[(Double, Double)]]): Gen[Map[(String, Double, Double), List[(Employee, Assignment, Salary)]]] =
    bracketsGen.flatMap(brackets =>
      employeesPerBracketGenerator.flatMap(employeesPerBracket =>
        brackets.map{ case (minIncome, maxIncome) =>
          employeeDataByDepartmentAndIncomeBracket(employeesPerBracket, department, minIncome, maxIncome).map(list =>
            ((department, minIncome, maxIncome), list))
        } foldToMap
      )
    )

  private val testScenarios: Gen[Map[(String, Double, Double), List[(Employee, Assignment, Salary)]]] =
    for {
      nBrackets <- Gen.choose(1, 10)
      department <- departments
      matchingEmployees <- employees(department, Gen.choose(2, 10), brackets(nBrackets, 5000.0, 20000.0))
    } yield matchingEmployees

  private implicit class FoldableToMap[K, V](complexStuff: List[Gen[(K, V)]]) {

    def foldToMap: Gen[Map[K, V]] = complexStuff.foldLeft(Gen.const(Map.empty[K, V]))((mapGenerator, partialGenerator) =>
      for {
        map <- mapGenerator
        pair <- partialGenerator
      } yield (map + pair)
    )
  }

  "EmployeeSalaryLens" should {

    "find employees by department, minimal and maximal salary" in forOneOf(testScenarios){ scenarios =>

      import sqlContext.implicits._

      val allEmployees = scenarios.flatMap{ case (_, list) => list.map{ case (employee, _, _) => employee}}.toSeq.toDF
      val allAssignments = scenarios.flatMap{ case (_, list) => list.map{ case (_, assignment, _) => assignment}}.toSeq.toDF
      val allSalaries = scenarios.flatMap{ case (_, list) => list.map{ case (_, _, salaries) => salaries}}.toSeq.toDF

      scenarios.foreach{ case ((department, minSalary, maxSalary), data) =>
        val results = EmployeeSalaryLens(department, minSalary, maxSalary)(allEmployees, allAssignments, allSalaries)
          .select("name", "surname", "id").as[(String, String, Long)].collect

        val expectedResults = data.map{ case (employee, _, _) => (employee.name, employee.surname, employee.id)}

        results should contain theSameElementsAs(expectedResults)
      }
    }
  }
}

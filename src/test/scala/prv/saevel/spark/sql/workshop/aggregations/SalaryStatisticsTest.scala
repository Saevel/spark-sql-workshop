package prv.saevel.spark.sql.workshop.aggregations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.hive.HiveContext
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalactic.TolerantNumerics
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import prv.saevel.spark.sql.workshop.{BasicGenerators, StaticPropertyChecks}
import prv.saevel.spark.sql.workshop.dataframes.{Assignment, Employee, Salary}

@RunWith(classOf[JUnitRunner])
class SalaryStatisticsTest extends WordSpec with Matchers with StaticPropertyChecks with BasicGenerators {

  case class DepartmentStatistics(department: String, averageIncome: Double, incomeStdDev: Double, employeeCount: Long)

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

  private implicit val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("SalaryStatisticsTest"))

  private implicit val sqlContext = new HiveContext(sparkContext)

  private implicit def tupleEncoder(implicit e1: Encoder[String], e2: Encoder[Long], e3: Encoder[Double]): Encoder[(String, Double, Double, Long)] =
    Encoders.tuple[String, Double, Double, Long](e1, e3, e3, e2)

  private val departments: Gen[String] = stringOfLength(10)

  def employeeDataByIncomeBracket(size: Int,
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

  private val departmentScenarios: Gen[(String, List[(Employee, Assignment, Salary)], DepartmentStatistics)] = for {
    department <- departments
    employeeCount <- Gen.choose(1, 10)
    data <- employeeDataByIncomeBracket(employeeCount, department, 5000.0, 15000.0)
  } yield (department, data, stats(data, department))

  private val testScenarios: Gen[List[(String, List[(Employee, Assignment, Salary)], DepartmentStatistics)]] =
    Gen.choose(1, 10).flatMap(n => Gen.listOfN(n, departmentScenarios))

  "SalaryStatistics" should {

    "calculate the mean salary, stardard deviance for salary and overall employee count for a given department" in {
      forOneOf(testScenarios){ scenarios =>
        import sqlContext.implicits._

        val employees = scenarios.flatMap{ case (_, data, _) => data.map{ case (employee, _, _) => employee}}.toDF
        val assignments = scenarios.flatMap{ case (_, data, _) => data.map{ case (_, assignment, _) => assignment}}.toDF
        val salaries = scenarios.flatMap{ case (_, data, _) => data.map{ case (_, _, salaries) => salaries}}.toDF

        val statsPerDepartment = SalaryStatistics(employees, assignments, salaries)
          .select($"department", $"salary_avg", $"salary_stddev", $"employee_count")

        scenarios.foreach { case (department, data, stats) =>
            val (_, averageSalary, salaryStdDev, employeeCount) = statsPerDepartment
              .where($"department" === department)
              .as[(String, Double, Double, Long)]
              .first

          averageSalary should equal(stats.averageIncome)
          salaryStdDev should equal(stats.incomeStdDev)
          employeeCount should equal(stats.employeeCount)
        }
      }
    }
  }

  implicit class DoubleSeqOps(data: Seq[Double]){

    def avg: Double = if(data.size == 0) Double.NaN else data.fold(0.0)(_ + _) / data.size

    def stdDev: Double = if(data.size == 0 || data.size == 1) Double.NaN else {
      val average = data.avg
      Math.sqrt(
        data.foldLeft(0.0)((accumulator, x) => accumulator + (x - average) * (x - average)) / (data.size - 1)
      )
    }
  }

  def stats(data: List[(Employee, Assignment, Salary)], department: String): DepartmentStatistics = DepartmentStatistics(
    department,
    data.map{case (_, _, salary) => salary.salary}.avg,
    data.map{case (_, _, salary) => salary.salary}.stdDev,
    data.size
  )
}

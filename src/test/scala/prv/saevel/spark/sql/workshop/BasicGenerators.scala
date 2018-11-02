package prv.saevel.spark.sql.workshop

import org.scalacheck.Gen

trait BasicGenerators {

  protected def stringOfLength(n: Int): Gen[String] = (0 until n)
    .map(_ => Gen.alphaChar.map(_.toString))
    .fold(Gen.const(""))((combined, nextGen) =>
      combined.flatMap(acc => nextGen.map(nextChar => acc ++ nextChar))
    )
}

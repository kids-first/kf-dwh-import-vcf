package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ACInput(ac: Long, an: Long)

case class ACOutput(af: BigDecimal)

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
  import spark.implicits._

  "af" should "return a a new column named ac with ratio of columns ac and an" in {
    val df = Seq(ACInput(1, 2)).toDS()
    df.select(calculated_af).as[ACOutput].collectAsList() should contain only ACOutput(0.5)
  }

  it should "return manage a precision of 8" in {
    val df = Seq(ACInput(5, 62976)).toDS()
    df.select(calculated_af).as[ACOutput].collectAsList() should contain only ACOutput(0.00007940)
  }

  "homozygotes" should "return homozygote count" in {
    val df = Seq(Seq(hom_11, hom_11, het_01, het_10, hom_00)).toDF("genotypes")
    df.select(homozygotes).as[Long].collect() should contain only 2
  }

  "heterozygotes" should "return heterozygote count" in {
    val df = Seq(Seq(hom_11, hom_11, het_01, het_01, het_10, het_10, hom_00)).toDF("genotypes")
    df.select(heterozygotes).as[Long].collect() should contain only 4
  }

  "colFromArrayOrField" should "return first element of an array" in {
    val df = Seq(
      Seq(1)
    ).toDF("first_array")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_array") as "first_col")
    res.as[Int].collect() should contain only 1

  }

  it should "return column" in {
    val df = Seq(
      1
    ).toDF("first_col")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_col"))

    res.as[Int].collect() should contain only 1

  }


}

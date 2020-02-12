package org.kidsfirstdrc.dwh.vcf

import org.kidsfirstdrc.dwh.vcf.util.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.kidsfirstdrc.dwh.vcf.util.Model._
case class ACInput(ac: Long, an: Long)

case class ACOutput(af: BigDecimal)

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import SparkUtils.columns._
  import spark.implicits._

  "af" should "return a a new column named ac with ratio of columns ac and an" in {
    val df = Seq(ACInput(1, 2)).toDS()
    df.select(af).as[ACOutput].collectAsList() should contain only ACOutput(0.5)
  }

  it should "return manage a precision of 8" in {
    val df = Seq(ACInput(5, 62976)).toDS()
    df.select(af).as[ACOutput].collectAsList() should contain only ACOutput(0.00007940)
  }

  "homozygotes" should "return homozygote count" in {
    val df = Seq(Seq(hom_11, hom_11, het_01, het_10, hom_00)).toDF("genotypes")
    df.printSchema()
    df.show(false)
    df.select(homozygotes).as[Long].collect() should contain only 2
  }

  "heterozygotes" should "return heterozygote count" in {
    val df = Seq(Seq(hom_11, hom_11, het_01, het_01, het_10, het_10, hom_00)).toDF("genotypes")
    df.printSchema()
    df.show()
    df.select(heterozygotes).as[Long].collect() should contain only 4
  }
}

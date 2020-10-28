package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.utils.SparkUtils.{fileExist, filename, union}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ACInput(ac: Long, an: Long)

case class ACOutput(af: BigDecimal)

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
  import spark.implicits._

  "zygosity" should "return HOM for 1/1" in {
    val df = Seq(hom_11).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HOM"
  }

  it should "return HET for 0/1" in {
    val df = Seq(het_01).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HET"
  }
  it should "return HET for 1/0" in {
    val df = Seq(het_10).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HET"
  }
  it should "return WT for 0/0" in {
    val df = Seq(hom_00).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "WT"
  }

  it should "return UNK otherwise" in {
    val df = Seq(unk).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "UNK"
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

  "filename" should "return name of the input files" in {
    val df = spark.read.json(getClass.getResource("/filename").getFile).select($"id", filename)
    df.as[(String, String)].collect() should contain theSameElementsAs Seq(
      ("1", "file1.json"),
      ("2", "file1.json"),
      ("3", "file2.json")
    )
  }

  "union" should "return a unioned df if both df are not empty" in {
    val df1 = Seq("1", "2").toDF("a")
    val df2 = Seq("3", "4").toDF("a")

    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq("1", "2", "3", "4")
  }

  it should "return df1 if df2 is empty" in {
    val df1 = Seq("1", "2").toDF("a")
    val df2 = spark.emptyDataFrame

    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq("1", "2")
  }

  it should "return df2 if df1 is empty" in {
    val df1 = spark.emptyDataFrame
    val df2 = Seq("3", "4").toDF("a")
    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq("3", "4")
  }

  "fileExists" should "return false if there is no file associated to the pattern" in {
    val path = getClass.getResource("/input_vcf/SD_123456").getFile

    val cgp = s"$path/*.unknown"
    println(cgp)
    fileExist(cgp) shouldBe false
  }

  it should "return true if there is files associated to the pattern" in {
    val path = getClass.getResource("/input_vcf/SD_123456").getFile

    val vcf = s"$path/*.CGP.filtered.deNovo.vep.vcf.gz"
    println(vcf)
    fileExist(vcf) shouldBe true
  }

}

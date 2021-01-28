package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarInput, ClinvarOutput}
import org.kidsfirstdrc.dwh.utils.ClassGenerator
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportClinVarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "run" should "creates clinvar table" in {

    withOutputFolder("output") { _ =>
      // test vcf file
      // val input = getClass.getResource(".").getFile
      // val inputDF = spark.read.format("vcf").load(input)

      val inputDF = Seq(
        ClinvarInput()
      ).toDF()

      val resultDF = ImportClinVar.transform(inputDF)

      val expectedResult = ClinvarOutput()



      resultDF.as[ClinvarOutput].collect().head shouldBe expectedResult

      }
  }

  "io case classes" should "be generated automaticaly from real data" in {

    val input = getClass.getResource("/input_vcf/clinvar.vcf").getFile

    val inputDF = spark.read.format("vcf").load(input)
      .where($"contigName" === "2" and $"start" === 69359260 and $"end" === 69359261)
      .withColumn("sampleId", lit("id"))
      .withColumn("genotypes", array(struct(col("sampleId") as "sampleId")))
      .drop("sampleId")

    val outputDf = ImportClinVar.transform(inputDF)

    val root = "src/test/scala/"
    ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarInput", inputDF, root)
    ClassGenerator.writeCLassFile("org.kidsfirstdrc.dwh.testutils.external","ClinvarOutput", outputDf, root)
  }

}

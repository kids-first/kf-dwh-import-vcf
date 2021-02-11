package org.kidsfirstdrc.dwh.update

import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.ClinvarOutput
import org.kidsfirstdrc.dwh.testutils.variant.Variant
import org.kidsfirstdrc.dwh.updates.UpdateVariant
import org.kidsfirstdrc.dwh.utils.Environment
import org.kidsfirstdrc.dwh.vcf.Variants
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try


class UpdateVariantSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  Try(spark.sql("DROP TABLE IF EXISTS variant.variants"))
  Try(spark.sql("DROP VIEW IF EXISTS variant.variants"))
  spark.sql("CREATE DATABASE IF NOT EXISTS variant_live")
  spark.sql("CREATE DATABASE IF NOT EXISTS variant")

  "transform method" should "return expected data given controlled input" in {

    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()
    val data = Map("variants" -> variantDF, "clinvar" -> clinvarDF)

    val job = new UpdateVariant(Environment.PROD)
    val resultDF = job.transform(data)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    // Checks the input values were not the same before the join
      variant.clinvar_id should not be Some(clinvar.name)
      variant.clin_sig should not be clinvar.clin_sig
    // Checks the output values are the same as expected
      resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "load method" should "overwrite data" in {

    val database = "variant"
    val rootFolder: String = getClass.getClassLoader.getResource(".").getFile
    println(s"output: $rootFolder")


    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()

    Given("existing data")

    val job = new UpdateVariant(Environment.PROD)

    new ImportClinVarJob(Environment.LOCAL).load(clinvarDF)
    job.load(variantDF, rootFolder)


    val data = job.extract(rootFolder)
    data("variants").show(false)
    data("clinvar").show(false)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    //runs the whole ETL job locally
    val resultJobDF = job.run(rootFolder, rootFolder)

    // Checks the job returned the same data as written on disk
    resultJobDF.as[Variant].collect().head shouldBe expectedResult

    // Checks the values on disk are the same as after the whole ETL was ran
    val resultDF = job.extract(rootFolder)(spark)("variants")
    resultDF.as[Variant].collect().head shouldBe expectedResult

    ////checks the hive table was published and up to date
    val variantsHiveTable = spark.table(s"$database.${Variants.TABLE_NAME}")
    variantsHiveTable.as[Variant].collect().head shouldBe expectedResult
  }

  "run for DEV" should "not publish" in {

    //Cleanup the data before running tests
    Try(spark.sql("DROP TABLE IF EXISTS variant.variants"))
    Try(spark.sql("DROP VIEW IF EXISTS variant.variants"))

    val database = "variant"
    val rootFolder: String = getClass.getClassLoader.getResource(".").getFile

    val variantDF = Seq(Variant()).toDF()

    val job = new UpdateVariant(Environment.DEV)
    job.load(variantDF, rootFolder)

    // checks that the result was not published
    Try(spark.table(s"$database.${Variants.TABLE_NAME}")).isFailure shouldBe true
  }

}


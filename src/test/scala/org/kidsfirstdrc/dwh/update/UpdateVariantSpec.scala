package org.kidsfirstdrc.dwh.update

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarOutput, TopmedBravoOutput}
import org.kidsfirstdrc.dwh.testutils.join.Freq
import org.kidsfirstdrc.dwh.testutils.update.Variant
import org.kidsfirstdrc.dwh.updates.UpdateVariant
import org.kidsfirstdrc.dwh.vcf.Variants
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class UpdateVariantSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  Try(spark.sql("DROP TABLE IF EXISTS variant.variants"))
  Try(spark.sql("DROP VIEW IF EXISTS variant.variants"))
  spark.sql("CREATE DATABASE IF NOT EXISTS variant_live")
  spark.sql("CREATE DATABASE IF NOT EXISTS variant")

  "transform method for clinvar" should "return expected data given controlled input" in {

    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()
    val data = Map(Clinical.variants -> variantDF, Public.clinvar -> clinvarDF)

    val job = new UpdateVariant(Public.clinvar, Environment.LOCAL)
    val resultDF = job.transform(data)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    // Checks the input values were not the same before the join
      variant.clinvar_id should not be Some(clinvar.name)
      variant.clin_sig should not be clinvar.clin_sig
    // Checks the output values are the same as expected
      resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "transform method for topmed" should "return expected data given controlled input" in {

    val variant = Variant()
    val topmed = TopmedBravoOutput()

    val variantDF = Seq(variant).toDF()
    val topmedDF = Seq(topmed).toDF()
    val data = Map(Clinical.variants -> variantDF, Public.topmed_bravo -> topmedDF)

    val job = new UpdateVariant(Public.topmed_bravo, Environment.LOCAL)
    val resultDF = job.transform(data)

    val expectedResult = variant.copy(topmed = Some(Freq(10, 5, 0.5, 5, 0)))

    // Checks the input values were not the same before the join
    variant.topmed should not be Some(Freq(10, 5, 0.5, 5, 0))
    // Checks the output values are the same as expected
    resultDF.as[Variant].collect().head shouldBe expectedResult
  }

  "load method" should "overwrite data" in {

    val database = "variant"

    val variant = Variant()
    val clinvar = ClinvarOutput()

    val variantDF = Seq(variant).toDF()
    val clinvarDF = Seq(clinvar).toDF()

    Given("existing data")

    val job = new UpdateVariant(Public.clinvar, Environment.LOCAL)

    new ImportClinVarJob(Environment.LOCAL).load(clinvarDF)
    job.load(variantDF)


    val data = job.extract()
    data(Clinical.variants).show(false)
    data(Public.clinvar).show(false)

    val expectedResult = variant.copy(clinvar_id = Some(clinvar.name), clin_sig = clinvar.clin_sig)

    //runs the whole ETL job locally

    val resultJobDF = job.run()

    // Checks the job returned the same data as written on disk
    resultJobDF.as[Variant].collect().head shouldBe expectedResult

    // Checks the values on disk are the same as after the whole ETL was ran
    val resultDF = job.extract()(spark)(Clinical.variants)
    resultDF.as[Variant].collect().head shouldBe expectedResult

    ////checks the hive table was published and up to date
    val variantsHiveTable = spark.table(s"$database.${Clinical.variants.name}")
    variantsHiveTable.as[Variant].collect().head shouldBe expectedResult
  }

}


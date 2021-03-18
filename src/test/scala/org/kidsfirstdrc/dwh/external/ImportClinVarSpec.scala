package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw.clinvar_vcf
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarInput, ClinvarOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportClinVarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  "run" should "creates clinvar table" in {

    withOutputFolder("output") { _ =>
      val inputData = Map(clinvar_vcf -> Seq(ClinvarInput()).toDF())

      val resultDF = new ImportClinVarJob(Environment.LOCAL).transform(inputData)

      val expectedResult = ClinvarOutput()

      resultDF.as[ClinvarOutput].collect().head shouldBe expectedResult

      }
  }

}

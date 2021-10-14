package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarInput, ClinvarOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImportClinVarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf(Catalog.kfStridesVariantBucket, getClass.getClassLoader.getResource(".").getFile)))

  val clinvar_vcf: DatasetConf = conf.getDataset("clinvar_vcf")

  "run" should "creates clinvar table" in {

    withOutputFolder("output") { _ =>
      val inputData = Map(clinvar_vcf.id -> Seq(ClinvarInput()).toDF())

      val resultDF = new ImportClinVarJob().transform(inputData)

      val expectedResult = ClinvarOutput()

      resultDF.as[ClinvarOutput].collect().head shouldBe expectedResult

    }
  }

}

package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{DddGeneCensusInput, DddGeneCensusOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportDDDGeneCensusSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  "run" should "creates ddd gene set table" in {

    val inputData = Map(Raw.ddd_gene_census.id -> Seq(DddGeneCensusInput()).toDF())

    val resultDF = new ImportDDDGeneCensus().transform(inputData)

    val expectedResult = DddGeneCensusOutput()
    resultDF.as[DddGeneCensusOutput].collect().head shouldBe expectedResult

  }

}

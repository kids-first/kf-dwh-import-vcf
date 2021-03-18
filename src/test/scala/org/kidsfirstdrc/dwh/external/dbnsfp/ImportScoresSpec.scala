package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.CatalogV2.Public
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.ImportScores._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportScoresSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant-parquet",
        getClass.getClassLoader.getResource(".").getFile)))

  "ImportScores" should "transform data into expected format" in {

    val inputDf = Map(Public.dbnsfp_variant -> Seq(Input()).toDF())
    val outputDf = new ImportScores(Environment.LOCAL).transform(inputDf)

    outputDf.as[Output].collect() should contain theSameElementsAs Seq(Output())

  }

}

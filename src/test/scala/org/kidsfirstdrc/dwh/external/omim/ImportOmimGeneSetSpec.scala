package org.kidsfirstdrc.dwh.external.omim

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{OmimInput, OmimOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportOmimGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  "ImportOmimGeneSet" should "read data into expected format" in {

    val outputDf = new ImportOmimGeneSet().extract()

    outputDf(Raw.omim_genemap2).as[OmimInput]

  }

  "ImportOmimGeneSet" should "transform data into expected format" in {

    val inputDf = Map(Raw.omim_genemap2 -> Seq(OmimInput()).toDF())
    val outputDf = new ImportOmimGeneSet().transform(inputDf)

    outputDf.as[OmimOutput].collect() should contain theSameElementsAs Seq(OmimOutput())

  }

}

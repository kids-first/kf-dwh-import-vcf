package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportScoresSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf(
        "kf-strides-variant",
        getClass.getClassLoader.getResource(".").getFile)))

  //"ImportScores" should "transform data into expected format" in {
//
  //  val inputDf = Map(Public.dbnsfp_variant -> Seq(Input()).toDF())
  //  val outputDf = new ImportScores(Environment.LOCAL).transform(inputDf)
//
  //  outputDf.as[Output].collect() should contain theSameElementsAs Seq(Output())
//
  //}

}

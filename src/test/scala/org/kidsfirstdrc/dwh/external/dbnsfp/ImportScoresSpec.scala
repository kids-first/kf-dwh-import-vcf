package org.kidsfirstdrc.dwh.external.dbnsfp

import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.ImportScores._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportScoresSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  "ImportScores" should "transform data into expected format" in {

    val inputDf = Map(Public.dbnsfp -> Seq(Input()).toDF())
    val outputDf = ImportScores.transform(inputDf)

    outputDf.as[Output].collect() should contain theSameElementsAs Seq(Output())

  }

}

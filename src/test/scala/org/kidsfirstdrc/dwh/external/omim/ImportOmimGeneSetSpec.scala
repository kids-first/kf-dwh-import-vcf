package org.kidsfirstdrc.dwh.external.omim

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.Omim
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportOmimGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  "ImportOmimGeneSet" should "transform data into expected format" in {

    val inputDf = Seq(Omim.Input()).toDF()
    val outputDf = ImportOmimGeneSet.transform(inputDf)

    outputDf.as[Omim.Output].collect() should contain theSameElementsAs Seq(Omim.Output())

  }

}

package org.kidsfirstdrc.dwh.external.omim

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{Omim, OmimInput, OmimOutput}
import org.kidsfirstdrc.dwh.utils.Catalog.Raw
import org.kidsfirstdrc.dwh.utils.Environment
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportOmimGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  "ImportOmimGeneSet" should "read data into expected format" in {

    val outputDf = new ImportOmimGeneSet(Environment.LOCAL).extract()

    outputDf(Raw.omim_genemap2).as[OmimInput]

  }

  "ImportOmimGeneSet" should "transform data into expected format" in {

    val inputDf = Map(Raw.omim_genemap2 -> Seq(OmimInput()).toDF())
    val outputDf = new ImportOmimGeneSet(Environment.LOCAL).transform(inputDf)

    outputDf.as[OmimOutput].collect() should contain theSameElementsAs Seq(OmimOutput())

  }

}

package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{CosmicCancerGeneCensusInput, CosmicCancerGeneCensusOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportCancerGeneCensusSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "run" should "creates cosmic gene set table" in {

    val inputData = Map(Raw.cosmic_cancer_gene_census -> Seq(CosmicCancerGeneCensusInput()).toDF())

    val resultDF = new ImportCancerGeneCensus(Environment.LOCAL).transform(inputData)

    val expectedResult = CosmicCancerGeneCensusOutput()
    resultDF.as[CosmicCancerGeneCensusOutput].collect().head shouldBe expectedResult

  }

}

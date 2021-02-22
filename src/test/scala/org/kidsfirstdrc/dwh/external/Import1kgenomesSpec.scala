package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.conf.Catalog.Raw
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{OneKGenomesInput, OneKGenomesOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Import1kgenomesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "run" should "creates 1000_genomes table" in {

    val inputData = Map(Raw.`1000genomes_vcf` -> Seq(OneKGenomesInput()).toDF())
    val resultDF = new Import1k(Environment.LOCAL).transform(inputData)
    val expectedResult = OneKGenomesOutput()
    resultDF.as[OneKGenomesOutput].collect().head shouldBe expectedResult

  }

}
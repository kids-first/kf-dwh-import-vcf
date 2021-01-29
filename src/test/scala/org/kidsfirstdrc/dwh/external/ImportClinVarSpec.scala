package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarInput, ClinvarOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportClinVarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  "run" should "creates clinvar table" in {

    withOutputFolder("output") { _ =>
      val inputDF = Seq(
        ClinvarInput()
      ).toDF()

      val resultDF = ImportClinVar.transform(inputDF)

      val expectedResult = ClinvarOutput()

      resultDF.as[ClinvarOutput].collect().head shouldBe expectedResult

      }
  }

}
